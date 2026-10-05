# Source Review — Modular Runtime and Operability

Reviewed 2026-10-05 against **`2aec51bb1e3c8122da7d496142ac8e25a21969a3`**
(tree `9a1d917627780908f670be0069d18fdebff7cb44`). This is a bounded source/
operability review, **not a penetration test, full-code proof or native fleet
qualification**. Findings distinguish reproduced behavior from source inspection.
[Modular Runtime](modularity.md) specifies changes; [Observability](observability.md)
specifies diagnostics. This documentation PR does not implement their fixes.

## 1. Rechecked baseline and retained progress

The previous audit used `db69c178`. Current main adds module-disconnect handling:
exact binding-generation `sending`/`native_accepted` Operations become
`outcome_unknown` transactionally, with restricted rootless-open reconciliation.
Keep that implementation and its old-link guard. Do not redo it or conflate it
with automatic process restart/native continuation.

Obtained the Ubuntu binary and source archive from CI run
[37298710850](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37298710850),
artifact `11341595767`. Archive SHA-256:
`0f845c0a7d3e2588c303586825ab9c0dc489633fbec09fe4a01af3ed6511faed`.
Its source commit and independently reconstructed Git tree matched the identities
above. No installed owner-machine services, models, accounts or private state
were used. Temporary host instances exited normally after the probe.

A fresh isolated IPC probe with distinct request IDs reproduced:

| Call / invariant | Observed result |
|---|---|
| Registered `manager` calls `task.create` | `FORBIDDEN` |
| Operator designates that client current GM, epoch 1 | Handover succeeds |
| Current GM calls new `task.create` and `task.revise` | Both `FORBIDDEN` |
| Operator submits four concurrent identical Task requests | One retained Operation/Task receipt |
| Same request ID, different origin payload | `REQUEST_ID_CONFLICT` |
| Observer attempts a Task write / reads status | Write denied / read succeeds |
| Manager repeats the same mailbox send | One receipt and one delivery |
| Host restart, repeat saved Task/message requests | Original identities preserved; DB contains one Task and one message Operation |

The passing receipt checks are a reason to retain the existing kernel rather
than rewrite it. The failed positive manager checks are product defects; green
negative-authorization tests do not compensate for an unusable manager path.
This probe is not a native inference or 200-agent test.

## 2. Findings and exact remedies

Links below are pinned to the reviewed source. Symbol names remain the preferred
migration anchors when later edits shift line numbers.

### F1 — High: manager/GM Task planning is denied (reproduced)

[`src/store/tasks.rs::create/revise`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/2aec51bb1e3c8122da7d496142ac8e25a21969a3/src/store/tasks.rs)
uses `p.require_operator()`. GM designation does not change a Manager's Role.
This contradicts normal Task planning in owner-decisions §5.1 and the manager
MCP surface. `Principal::require_writer` is broader than a positive manager
allowlist, so a global replacement with it is not the remedy.

**Change:** one ordinary Operator/Manager admission policy, preserve the existing
revision check, and add the missing guard against an ordinary Manager revising
another Manager's unreleased Attempt. No native-readiness requirement for Task planning.
Keep module/participant/hook/observer roles restricted and current-GM authority
for genuinely GM-only actions. Define active foreign-Attempt revision behavior
as in [Modular Runtime §4](modularity.md#4-less-authorization-ceremony-one-effective-policy).
**Acceptance:** reproduce the table above after the fix; the manager-positive
calls succeed, unauthorized-role and duplicate-request checks still behave correctly.
An ordinary Manager's revision of a foreign unreleased Attempt is denied; its
owner and the explicit current-GM/Operator path retain their documented rights.

### F2 — High: optional supervisor failure stops the host (source-confirmed)

[`src/host.rs::run_until`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/2aec51bb1e3c8122da7d496142ac8e25a21969a3/src/host.rs)
unconditionally starts ten supervisors. Any early supervisor completion/error or
JoinError breaks the host loop, drops the listener and signals all supervisors to
stop. This is host-level fate sharing; it does **not** prove all native processes
are killed. No crash injection was run in this review.

**Change:** move optional workers behind the independent supervisor/process
contract; classify local worker failure separately from kernel/DB failure.
Record and restart only the needed failed worker with bounded backoff/readback.
**Acceptance:** a deliberately failed optional adapter/recorder does not change
kernel boot/PID or interrupt another adapter's admitted work; no input replay.

### F3 — Medium: unused optional loops still run (source-confirmed)

The same host starts optional supervisors before checking actual demand.
`supervise_automation`, launcher and native-MCP helpers have recurring ticks;
several are two-second intervals. This is nonzero polling even for an empty
configuration, **not evidence that unused models are loaded**.

**Change:** explicit enabled/demand predicates, one due queue, shared event
notification and single-flight startup; quiescent optional workers exit safely.
**Acceptance:** disabled modules have no worker/timer/provider connection; active
native sessions and unresolved effects are not destroyed by an idle optimization.

### F4 — High architectural defect: adapter changes require core knowledge

[`src/store/operations.rs::reserve_workspace_open`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/2aec51bb1e3c8122da7d496142ac8e25a21969a3/src/store/operations.rs)
selects `directory`, `workdir` or `workspaceRoot` by runtime name and rejects an
unknown runtime. [`src/store/runtime.rs`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/2aec51bb1e3c8122da7d496142ac8e25a21969a3/src/store/runtime.rs)
and [`src/store/opencode.rs`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/2aec51bb1e3c8122da7d496142ac8e25a21969a3/src/store/opencode.rs)
also retain provider interpretation. Prerequisite-validator extraction was real,
but did not make the whole Store provider-neutral.

**Change:** adapter-owned workspace/native-option/observation validators behind
versioned IPC; kernel only generic identities, evidence envelopes and transitions.
**Acceptance:** register a differently shaped executor without editing Store or
another adapter. Keep the existing safe prerequisite receipt/barrier.

### F5 — High operability gap: no independent compilation/deployment boundary

[`Cargo.toml`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/2aec51bb1e3c8122da7d496142ac8e25a21969a3/Cargo.toml)
has the large application package and an empty workspace declaration for parent
workspace isolation, not the required product package graph. `src/lib.rs` links
all owned subsystems; Justfile's build is a full optimized `swarm` binary.

**Change:** the package/process map in Modular Runtime §2, no adapter dependencies
in kernel/frontends, scoped build recipes and separate iterate/release profiles.
**Acceptance:** adapter-only change builds/replaces that adapter and necessary
libraries, with unchanged kernel/other-adapter artifacts. No timing improvement
is claimed before like-for-like build measurements.

### F6 — Medium: disconnect persistence errors are hidden (source-confirmed)

[`src/store/mod.rs::Store::disconnected`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/2aec51bb1e3c8122da7d496142ac8e25a21969a3/src/store/mod.rs)
uses `.await.unwrap_or(false)` and returns no error. A failed queued DB transition
therefore becomes indistinguishable to this caller from no changed rows. The new
transaction and change notification are useful; hiding failure is not. No disk/
DB fault was injected, so this is not a claim that production data was lost.

**Change:** retain the error, emit a bounded named diagnostic and mark affected
health/reconciliation work; return/propagate it to the supervising owner without
escalating one optional reader failure into a global stop. Store errors still
prevent uncommitted work from being acknowledged. Do not retry external input.
**Acceptance:** a failed persistence attempt is visible with operation/binding/
module scope; successful and stale-old-link disconnect behavior remains intact.

### F7 — Medium: diagnostics are not a configurable live logging subsystem

[`src/host.rs`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/2aec51bb1e3c8122da7d496142ac8e25a21969a3/src/host.rs)
uses `eprintln!`; the manifest has no direct tracing subscriber/recorder setup.
Durable reports, Doctor and selected native diagnostic artifacts exist, but do
not implement the required correlated, adjustable logging/live-query service.

**Change:** implement Observability's small producer early, then independent
recorder/filter reload/follow/metrics. Never put unbounded native text into the
transactional event bus or use logs as new Task authority.
**Acceptance:** show a child/module failure and its recovery with exact IDs/time,
change depth live, and keep work running with recorder absent/saturated.

### F8 — Medium: documentation and CI coverage drift (source-confirmed)

`docs/mcp-profiles.md` said 53 tools/22 reads/31 writes; `src/mcp.rs`'s closed table
has **120/54/66** at this source. The profile list is not proof a granted role can
actually execute each method. Historical qualification counts must remain dated.
`THIRD_PARTY_NOTICES.md` omitted the enabled RMCP streamable-HTTP feature.

[`.github/workflows/rust.yml`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/2aec51bb1e3c8122da7d496142ac8e25a21969a3/.github/workflows/rust.yml)
has push-main/manual triggers, no PR trigger, and excludes tests/tools-only changes.
**Change:** correct current docs now; generate catalog facts from one registry;
implement changed-package PR checks and relevant path coverage during M2.
**Acceptance:** no hand-maintained conflicting totals; docs-only changes do not
launch models and adapter-only CI does not rebuild unrelated products.

## 3. Risk checks required while extracting modules

These are design hazards to prevent/verify, **not vulnerabilities demonstrated
against the current deployment**:

| Risk | Required control without extra approval ceremony |
|---|---|
| Duplicate process/work after reconnect | Durable Operation and exact old/new boot/generation; single-flight start; GET-only unknown-effect recovery. |
| Module impersonation / stale replies | Private IPC, retained assigned identity, positive method capability, generation checks; no shared operator bearer. |
| Unsafe executable/config replacement | Allowlisted local artifact location, separate argv, versioned atomic selection, no command string from an event payload. |
| Broken transaction after extracting bus | Journal stays in Store; action/pending state and cursor commit together via typed kernel admission. |
| Log/stream memory and disk exhaustion | Byte/record bounds, retention, priority and visible drops; recorder cannot stall the control path. |
| Secret leakage through debug/trace | Redact before persistence/fanout, including split chunks; scope before pagination; safe terminal rendering. |
| Native children killed by worker upgrade | Verify ownership; bridge lifetime differs from native owner; preserve admitted process groups/sessions. |
| Silent loss of late results after revocation | Separate permission for new effects from authenticated collection of an old admitted outcome. |
| Broken rollback/migration | Preserve old executable and database evidence; supported migration/backup; no empty-state fallback. |

No assertion of complete security, no public listener deployment, no real secret
injection and no broad native fault/load campaign was performed. The previous
1000-events/s and 200-native-agent goals remain unqualified; IPC clients are not
native agents. Current source review does not convert the historical C24/C35
inventory runs into productive model or complete O7 workflow evidence.

## 4. Documentation and implementation separation

This PR defines the new module/build/restart and observability contracts, updates
the operations entrypoint/donor evidence and corrects current profile/notices.
The existing executable, permissions and process topology are unchanged by it.
M1–M6 are implementation work, with exact touched areas and completion criteria,
not six additional runtime phases or approval gates. Existing frozen policy
editions and historical source/acceptance evidence must remain byte-identical.
