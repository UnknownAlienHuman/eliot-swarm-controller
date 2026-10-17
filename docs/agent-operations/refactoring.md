# V3 refactoring map

Source baseline: `f1b854b6fd058dc2440d3848fc95acc279657d3f`, 2026-10-17.
Its parent is `27fcae1ed2d9fc0aa778680d5e39a08834248023`. The V3 commit changes
11 documentation files and no runtime source. This map turns the existing O1–O11
and M1–M6 requirements into connected source slices; it adds no approval stage,
activation ledger or separate backlog.

## 1. Authority and audit scope

[V3](README.md) is the target for ongoing development, including donor reuse,
productive result collection, direct launch, relational state and concurrent reads.
[V18](../agent_swarm.md), the [module contract](../agent_swarm.module-contract-v2.md)
and accepted policy editions retain the unaffected transactional and ownership
contracts. Changing an implementation does not rewrite frozen Attempts, request
receipts or historical artifact bytes. Current source determines what exists;
[implementation status](../implementation-status.md) records the scope of its evidence.

The relevant implementation audit is [Issue #153](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/153),
including Wave 2 and subsequent corrections. Its `ed1df44` findings are dated;
the [closeout map](../remediation/2026-10-15/ISSUE-153-CLOSEOUT.md) records later work.
The older [CURRENT-AUDIT](../remediation/2026-10-09/CURRENT-AUDIT.md) and
[modularity review](modularity-review-2026-10-05.md) are evidence at their own revisions.
This preparation inspected manifests and the critical Store, launch, result,
worker and journal paths. It is not an exhaustive line-by-line audit or a new
runtime/qualification pass.

A reproducible size baseline is 479 owned `.rs` files / 534,926 physical lines,
including comments, blank lines and tests, excluding `vendor/`; 366,932 lines are
under `crates/swarm-kernel-host`. Files whose path contains `tests` or whose stem
contains `test`/`fixture` account for 85,087 lines; other files also contain inline
tests. These counts are navigation data, not production SLOC or measured runtime
cost. The structural problem is responsibility and dependency concentration.

## 2. Audit findings already addressed

Verify these current symbols before assigning new work; historical failing receipts
remain historical. Source presence does not imply a current full-suite pass.

| Audit item | Current source evidence | Disposition |
| --- | --- | --- |
| D1: qualification helpers broke normal test compilation | [`swarm-automation`](../../crates/swarm-automation/src/lib.rs): both qualification-admit test modules require `qualification-faults`. | Source correction present. |
| D3 / #150: CheckRun ID used as Operation ID | [`acceptance.rs`](../../crates/swarm-kernel-host/src/store/acceptance.rs): selects `s.operation_id`, then loads that Operation while retaining `cached_from` as CheckRun ID. | Source correction present. |
| D5: #146 / #148 / #149 | [`launcher.rs`](../../crates/swarm-kernel-host/src/store/launcher.rs): immutable reconcile cursor; [`results.rs`](../../crates/swarm-kernel-host/src/store/results.rs) / [`object_scope.rs`](../../crates/swarm-kernel-host/src/store/object_scope.rs): assembly cache; [`retention.rs`](../../crates/swarm-adapter-opencode/src/journal/retention.rs): durable removal-uncertainty marker. | Preserve the corrections and their stated limits. |
| Dead WorkQueue / DemandGate | [PR #154](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/154) merged; [`swarm-bus`](../../crates/swarm-bus/src/lib.rs) retains the used event/router contract. | Deleted; no replacement queue needed. |
| Repeated Command / Claude SHA predicates | [`Command lib.rs`](../../crates/swarm-adapter-command/src/lib.rs): `is_lower_sha256_hex`; [`Claude receipt.rs`](../../crates/swarm-adapter-claude/src/receipt.rs): shared `is_sha256`. | The named package-local cleanup is present. |
| Duplicate protected-artifact and ScriptRun JSON digests | [`Sha256Digest::of_canonical_json`](../../crates/swarm-contracts/src/module_catalog.rs), [`script_run_canonical_json_*_v1`](../../crates/swarm-scripts/src/lib.rs) and their producer/consumer call sites. | Shared contract owners exist; use them. |

D4's isolation/readback remainder stays domain-specific. A missing SAVEPOINT in an
arbitrary file does not prove a fleet stall; trace selection, isolation and the
caller before assigning a defect. Existing fixes do not prove universal recovery.

## 3. Current gaps against V3

| Area | Verified baseline | Required change |
| --- | --- | --- |
| Read dispatch | [`Store::call`](../../crates/swarm-kernel-host/src/store/mod.rs) sends only `host.status`, `monitor.snapshot` and `monitor.follow` to [`status_reader`](../../crates/swarm-kernel-host/src/store/status_reader.rs): one connection, one FIFO. Most other SQL reads use `RunJob`. | Bounded WAL reader concurrency and explicit query-family routing. |
| Capacity | [`capacity.rs`](../../crates/swarm-kernel-host/src/store/capacity.rs): v2 stores a scope header plus individual JSON entry rows in `meta`; v1 whole-ledger compatibility remains. | Indexed relational capacity records. The audit's single-blob description is no longer the whole current path. |
| Other domain state | [`automation/config.rs`](../../crates/swarm-kernel-host/src/automation/config.rs) and [`store/automation.rs`](../../crates/swarm-kernel-host/src/store/automation.rs) encode entry and dispatch records in `meta`. | Migrate one record family with its indexes and consumers at a time. |
| Package boundaries | [`host Cargo.toml`](../../crates/swarm-kernel-host/Cargo.toml) directly links six worker implementations: bus, automation, script-worker, supervisor, checks, observer. `swarm-scripts` also reaches bus. | Remove implementation edges, including transitive edges, through existing contract/client/process owners. |
| OpenCode result | [`native.rs::read_assistant_result`](../../crates/swarm-adapter-opencode/src/native.rs) and [`lib.rs::normalized_assistant_result_page`](../../crates/swarm-adapter-opencode/src/lib.rs) still require an explicit assistant parent. | Implement V3 serialized-turn association for parentless streams. |
| Launcher readiness | [`launcher_dispatch.rs`](../../crates/swarm-kernel-host/src/store/launcher_dispatch.rs) requires current C8 tool proof for launcher-owned OpenCode work. Direct dispatch without a launch parent is already independent. | Make native MCP observation asynchronous in the V3 launch path. |
| Donor integration | `rmcp`, ACP, `croner` and Atlas have production consumers. `tracing` is only transitive; octocrab, notify, sysinfo, tracing-subscriber/appender and CCCC are absent from direct dependencies. | Integrate at the named boundary, replacing its current mechanism. See [donor map](donor-map.md). |

**Two result/readiness corrections:** Antigravity already records a single-flight
Operation → receipt/conversation/ordinal/boot/generation mapping and returns its
retained body ([controller](../../modules/antigravity-rust/src/controller.rs),
[result page](../../modules/antigravity-rust/src/result_page.rs)); its old guide
was stale. Preserve this implementation. Codex uses explicit native input/turn
identity; Claude uses its native user-message UUID evidence. Do not weaken those
working paths to a generic latest-message selector.

The current OpenCode C8 prerequisite is an availability restriction, not a proven
circular model-request deadlock: the plugin's `arm` path calls `McpService.tools()`
before a prompt, and the consumer accepts unknown provider-request evidence
([plugin](../../modules/opencode/native-mcp-proof.mjs),
[validator](../../crates/swarm-kernel-host/src/runtime/opencode_v2/mcp_tools.rs)).
V3 still requires removing that prerequisite from initial prompt admission.
The literal `dispatch_permitted: false` is an evidence-field invariant; deleting
that comparison alone does not remove the actual prerequisite.

## 4. Connected work order

Start with the productive path. Store, worker and donor slices can proceed in
parallel on non-overlapping assignments after their manager fixes shared-file
ownership. Native qualification is a later phase, not a prerequisite for source
work. Existing O/M identifiers remain the scope map; the rows below are small
candidate boundaries, not new permanent Issues.

| First candidate | Scope / starting files | Completion and deletion |
| --- | --- | --- |
| O3: OpenCode result association | Adapter `native.rs`, `lib.rs`, journal/result context; shared origin DTO only if required. | Capture one admitted turn, return its bounded normalized body, retain its exact Operation association. Remove the parent-only rejection for this supported path; retain explicit native lineage when supplied. |
| O3/O10: direct launcher dispatch | `store/launcher_dispatch.rs`, `launcher.rs`, native-MCP reconciliation and public progress projection. | Ready binding accepts the first dispatch with valid Task/workspace/caller scope; MCP observations arrive asynchronously. Remove the `require_current_dispatch_capability` admission dependency and its duplicate pre-effect readiness gate, retaining ancestry/receipt checks. |
| M2/O2: WAL readers | `swarm-store`, `status_reader.rs`, `Store::call` and selected read handlers. | Existing status/monitor plus a first bounded query family use concurrent read-only snapshots; delete their writer-queue branches. Expand method families separately as below. |
| M2/O1: relational capacity | Existing schema installer and `store/capacity.rs`. | Convert scopes with held reservations intact, switch all scope reads/writes to indexed rows, then retire migrated `capacity:*` / `capacity-entry:*` representations. |
| M2/O1–O2: relational automation | `automation/config.rs`, `store/automation*.rs`, existing schema installer. | Entry/config first, then cursor/pending/review families in separate candidates; remove each replaced key codec and scan when its consumers switch. |
| M2–M4: worker boundaries | The edge-removal table below. | Each worker-only edit leaves the host outside its actual Cargo reverse-dependency closure; one production owner remains. |
| O4: GitHub donor | Existing `github/client.rs` traits plus issue/label/PR consumers. | One shared octocrab backend replaces `run_gh_api`; current account/host, bounded responses and Store-owned write/readback survive. Native Git publication keeps its own process boundary. |
| M5/O2: observation donors | `swarm-telemetry`, `swarm-observer`, config/watch and `swarm-process`. | tracing replaces duplicate logging plumbing; notify replaces matching file polling; sysinfo supplies metrics; CCCC supplies suspended-child bootstrap where that launch path exists. Complete one donor/consumer replacement per candidate. |

### WAL migration detail

Generalize the existing reader path; `swarm-store::open_reader` already opens
read-only/query-only connections. Keep a bounded worker-owned pool, one connection
per executing request and no unbounded thread/task creation. Resolve the current
principal and read its authorized projection in the same short `DEFERRED` read
transaction. Preserve each handler's candidate scan, per-row authorization and
visible-page ordering in that snapshot. For observation-position pages,
unauthorized or missing rows advance raw coverage without consuming a visible
slot; preserve `next_after`, gap and coverage semantics. Pool saturation uses
bounded backpressure rather than a hidden return to the writer queue.

Begin with existing status/monitor, `runtime.catalog`, `module.catalog.get`,
`check.profiles` and `route.list`; then Task/Attempt/review reads, Operation
projections, reports and automation explain. Audit actual handlers, not method
suffixes. `module.next` changes durable dispatch state; `stream.open/read/close`
manages an in-memory stream registry. `artifact.read` performs DB authorization
and cache-epoch validation before file I/O; `artifact.parts` is a bounded DB-only
projection with per-page authorization and assembly-cache snapshot validation.
Move those artifact paths only after making their immutable read snapshots
explicit. Keep schema changes and durable cursor advancement on the writer.

A single RPC snapshot includes authorization and data; where a handler exposes a
journal cut, capture it in that same snapshot. Separate page RPCs retain their
existing cursor/gap semantics; do not keep a read transaction
open across clients or stream lifetime. WAL allows read/write concurrency but
long readers can delay checkpoints ([SQLite WAL](https://sqlite.org/wal.html)).
No throughput improvement is claimed until it is measured.

### Relational conversion detail

Use `Store::initialize_database` / `install_schema_extension` and the existing
`crates/swarm-kernel-host/migrations/` directory. `swarm-store` owns connection
mechanics, while Store owns domain DDL and conversions; no second migration runner.
Capacity rows need explicit scope/entry keys, identity, state and indexes used by
reservation and reporting queries. Keep Operation/Attempt records authoritative.

Create tables once, then convert bounded scopes on the writer with a resumable
cutover marker and a single authoritative representation per scope. Validate
entry counts, identities and reservation totals before cutover; preserve damaged
source for the existing scoped isolation/readback path. Never treat unknown
capacity as free. Keep v1/v2 reads until the relevant saved scopes are migrated;
remove old key writes/rows when their cutover is committed. Do not introduce
permanent dual-write ledgers or migrate data from a read request.

### Remove worker implementation edges

| Current host edge | Existing destination for the shared seam | Worker-only remainder |
| --- | --- | --- |
| `swarm-supervisor` | `swarm-contracts`: bootstrap/health/control DTOs; `swarm-client`: bounded control client; `swarm-process`: owner primitives. | Standalone supervisor registry, spawn/restart loop and child lifecycle. Host keeps Store demand/receipt coordination. |
| `swarm-bus`, including through `swarm-scripts` | `swarm-contracts`: event/selector contract and pure matching used by Store/script planning. | Dispatcher process and wake loop. Retain `route_event`/`same_slot` behavior; the dead queue is already gone. |
| `swarm-script-worker` | `swarm-scripts`: ScriptRun wire/plan/start-decision contract; `swarm-process`: file/process primitives. | Executor loop, child execution and captures. Remove the host-linked compatibility entrypoint and dependency after the standalone consumer is connected. |
| `swarm-checks` | `swarm-contracts`: check plan/result DTOs; `swarm-process`: bounded process/capture mechanics. | Check execution. Preserve the current standalone-host admission and custody path. |
| `swarm-automation` | `swarm-contracts`: worker config/ready facts; `swarm-process`: exact family ownership/readback. | Scheduler worker loop. Store keeps atomic slot/admission decisions and `croner` supplies calendar parsing. |
| `swarm-observer` | `swarm-telemetry`: small in-process producer/config contract; shared process facts in `swarm-process`. | Recorder, follow/metrics CLI and observer worker. Preserve recording when another sink fails. |

Move one seam with both callers, then remove its Cargo edge. Do not publish empty
wrapper crates or copy worker implementations into the host under another name.
Use Cargo metadata to check the full local normal-dependency graph, including
`swarm-scripts → swarm-bus`. Small `cfg(test)` units are already excluded from
production builds; moving tests alone does not establish this boundary or a
runtime-performance gain. Relocate large integration fixtures as their seam moves.

## 5. Compatibility, journals and donor completion

Apply the [schema and identity rules](architecture.md#schema-and-identity-boundaries)
at the owning DTO. Optional projection metadata must not break readback; fields
that change authority, required capabilities or an existing digest format need
the corresponding versioned contract. Preserve raw TaskPrompt/artifact bytes and
use current canonical helpers for the contracts they own. No global SHA/serde/JCS
rewrite is needed to deliver V3.

A common journal state engine is not justified by the current formats: Claude and
OpenCode have different JSONL reducers; Command uses immutable per-Operation files;
Muse uses a checkpoint; Codex uses sequenced snapshots and checksum files.
[`swarm-process::scan_jsonl`](../../crates/swarm-process/src/jsonl.rs) and
[private durable-file helpers](../../crates/swarm-process/src/permissions.rs)
already own shared mechanics. A bounded append helper with Claude and OpenCode
as real consumers is a possible later deletion; keep each adapter's record,
ACK, torn-tail and retention semantics. It is not a prerequisite for results.

For octocrab, preserve the existing host/account/credential selection and the
4 MiB pre-materialization response limit. Disable implicit write retries in the
selected SDK and lower transport; uncertain effects reconcile through existing
Store paths. Keep ELIOT authorization, selected-source, watermark and publication
semantics outside the SDK ([audited migration boundary](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/153#issuecomment-6169520482)).
For tracing, preserve record/byte limits, cross-chunk redaction and independent
sink failure handling; the appender's line limit is not an aggregate-byte budget.
For CCCC, integrate its complete suspended-child primitive; its reviewed crate
does not implement ELIOT's full Job/family policy. Preserve owner-first non-killing
observation and separate bounded-check termination semantics.

## 6. Completion and verification

A source candidate delivers its producer, handler, registration/projection and
consumer together, removes the replaced path, and updates the existing contract
and status entry. Each manager integrates before one scoped format/minimal strict
Clippy pass with actual reverse dependencies and an external shared Cargo target.
Documentation-only candidates run the documentation gate. Subagents run no Cargo.

Tests, process/native exercises, load measurements and full product qualification
follow completed source. Retain their exact source/platform/scope when that phase
runs. Source work does not wait for paid inference, optional services, all-adapter
parity or a new audit campaign. Do not repeat unchanged failing qualification or
add a large verification document per attempt. Current productive/native gaps
remain visible without being prerequisites for unrelated implementation.
