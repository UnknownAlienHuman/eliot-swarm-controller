# Current audit register

This is the active continuation register. AR-001–078 remain in the
[historical register](audit-register.md). Later ranges are frozen at their merge
checkpoints:

- AR-079–088 at [`b7a5fe1`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b7a5fe164a50cbefe2ca315c8f3a7f209d1c0d9b/docs/agent-operations/audit-current.md);
- AR-089–099 at [`e42c464`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/e42c464d58db770cc5490f46f391843f9fec8df2/docs/agent-operations/audit-current.md);
- AR-100–114 at [`e99c06b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/e99c06bb490fe630eec70337ce5137a26e145d84/docs/agent-operations/audit-current.md);
- AR-115–126 at [`053f1e0`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/053f1e0fb15b3d40c00918f4a25ec8f9e4969e02/docs/agent-operations/audit-current.md).

[Audit index](audit-index.md) maps every range to immutable evidence;
[implementation](implementation.md) owns source changes;
[IPC/MCP](ipc-mcp.md) owns the target transport/read contract; and
[status](../implementation-status.md) owns landed behavior.

## Resume here

- Checkpoint: **2026-10-24 / IPC, MCP concurrency, subscriptions and hot reads**.
- Source inspected at **`053f1e0fb15b3d40c00918f4a25ec8f9e4969e02`**.
- Connected chain:
  `swarm-client::{Client,Session}` → kernel `ipc::serve_connection` →
  `swarm-mcp::McpFacade::request_on` and profile authorization →
  MCP Tasks projection → subscription hub/pollers/forwarders →
  `report.delta`, `report.attention`, `operation.get` → Store writer queue and the
  existing query-only WAL status reader.
- Overall audit: **incomplete, 141 entries**. AR-127–141 are recorded below; prior
  dispositions remain in immutable checkpoints.
- Preserved in this pass: one authenticated physical link; concurrent ID-correlated
  exchanges; bounded in-flight capacity; no application request before hello; no blind
  retry after possible write; all unresolved calls become unknown on transport damage;
  concurrent host intake; accepted work drains after disconnect; live MCP authorization;
  separate tool/pump links; bounded notification bytes/depth and explicit lag markers;
  mutation success is never turned into failure by optional task projection.
- Open in this pass: MCP holds an outer connection mutex for the full RPC and defeats
  client multiplexing; up to sixteen subscriptions run independent report-delta pollers;
  `tasks/get` scans every page of global attention to find one binding; attention builds
  the complete current item vector before pagination; report/subscription reads repeat
  authorization/projection work on the single Store writer; coordination categories and
  resync methods remain embedded in the subscription product.
- Selected runtime slice: clone the current shared `Client` under the slot lock, release
  the lock before `request_shared`, and clear only a still-disconnected slot after a
  transport-class failure. A late failure must never clear a newly connected healthy
  client. This is **not yet landed** at this checkpoint.
- **Next exact chain:** local Governor observation:
  binding-rooted `module_observations` and provider conditions → adapter/provider-local
  readers → process metrics/supervisor process projection → workspace lease versus live
  Git facts → publication-only remote readback → one joined current view with unmatched
  subjects and explicit source gaps.
- Runtime implementation order remains **A1b–A3, then B–V**. Source-audit order does
  not reorder implementation.
- PR #189 is merged at `053f1e0`. Exact head `3c05253`, workflow run `40088525487`,
  completed successfully; it changed documentation only and made no Store migration or
  database-qualification claim.
- Still unqualified: concurrent MCP calls through the facade, reconnect races, slow
  transport and cancellation under load, coalesced subscription delivery, WAL routing
  for ordinary reads, writer fairness, large attention/report history and installed
  autonomous operation.

On resume, compare live `main` with the inspected head, reopen only changed owners and
consumers, then continue the named observation chain. A documented concurrency target
is not a landed runtime fix.

<a id="findings-ar-127141"></a>
## Findings AR-127–141

| ID | Requirement / path | Disposition | Remaining boundary |
| --- | --- | --- | --- |
| <a id="ar-127"></a>AR-127 | Authenticated shared IPC client | **Preserve.** No public `Client` is returned before `client.hello`; clones share one socket, reader/writer, pending map and failure state. | Keep one physical authentication and never reconnect implicitly inside a logical request. |
| <a id="ar-128"></a>AR-128 | Concurrent request correlation | **Preserve.** `Session` allocates unique connection-scoped IDs, bounds in-flight requests and matches out-of-order replies to exact waiters. Application refusal does not poison the link. | Keep ID retirement and capacity semantics while exposing concurrency through MCP. |
| <a id="ar-129"></a>AR-129 | Unknown outcome and cancellation | **Preserve.** Pre-write cancellation sends no frame; cancellation after possible write, malformed/mismatched response, partial EOF or writer failure poisons the link and fails all unresolved calls `OUTCOME_UNKNOWN`. No request is replayed. | Frontends may reconnect only for a later command; original mutation resolves through Store readback. |
| <a id="ar-130"></a>AR-130 | Host connection concurrency | **Preserve.** After authenticated hello the host accepts up to configured in-flight requests, runs them in a `JoinSet`, serializes only response writes and drains accepted application work after disconnect. | Store-domain serialization/fairness remains separate from transport concurrency. |
| <a id="ar-131"></a>AR-131 | MCP tool-call connection lock | **Open root cause.** `request_on` holds `Mutex<Option<Client>>` from connect through the entire awaited RPC and calls the sequential `request(&mut self)`. The already-multiplexed client is therefore serialized by the facade. | Clone the shared client under lock, release lock, call `request_shared`, then generation-safely clear only a disconnected slot. |
| <a id="ar-132"></a>AR-132 | Reconnect race | **Open cut condition.** A transport failure must make the next command reconnect without retrying the failed request, but a late failure from an old client must not erase a newer healthy slot. | Recheck slot connectivity/identity under lock after failure; never clear a live replacement. Add concurrent disconnect/reconnect coverage. |
| <a id="ar-133"></a>AR-133 | Tool/pump isolation | **Preserve.** Tool calls and subscription polling use different authenticated IPC connections, so a stalled/dead pump link cannot directly wedge a tool call and vice versa. | Keep two responsibility links; remove serialization within each link and avoid multiplying pump connections. |
| <a id="ar-134"></a>AR-134 | Subscription polling fan-out | **Open amplification.** Every subscription owns a poller, forwarder and queue. Up to sixteen pollers independently establish/read the same `report.delta` stream every 250 ms and serialize page requests on one pump mutex. | One session delta/wake pump reads each committed range once and fans out by category to bounded subscriber queues. |
| <a id="ar-135"></a>AR-135 | Slow-subscriber semantics | **Preserve.** Queue depth and aggregate bytes are bounded; the in-flight notification retains its byte permit; overflow emits one explicit lag episode with skipped cursor range/count and exact recovery reads; failed reads do not advance the cursor. | Retain these semantics in the coalesced pump; one slow subscriber must not stall another. |
| <a id="ar-136"></a>AR-136 | Subscription product leakage | **Open deletion dependency.** Categories and recovery reads include Concilium and coordination Thread/contract products, duplicating the platform marked for removal in AR-100–113. | Retain reports/mailbox/operations invalidation only after mailbox normalization; delete coordination categories, schemas, profile requirements and resync routes vertically. |
| <a id="ar-137"></a>AR-137 | MCP Tasks exact pending input | **Open hot read.** `tasks/get` reads exact `operation.get`, then pages the entire global `report.attention` projection and filters locally by binding/generation. One task read can traverse all fleet attention. | Add one Store-authorized exact pending-input read keyed by Operation/binding generation; MCP performs no fleet reduction. |
| <a id="ar-138"></a>AR-138 | Attention pagination | **Open hidden materialization.** `report.attention` builds the full current attention vector before applying the requested page. Pagination bounds the response, not Store work or writer occupancy. | Query exact/indexed current subjects or use bounded keyset production before materialization; preserve explicit partial/gap semantics. |
| <a id="ar-139"></a>AR-139 | Timeline/subscription read cost | **Open repeated work.** `report.delta` scans a bounded global Observation candidate page, resolves per-item grants and projection frames, and every subscription poller repeats that work on the Store writer. | One coalesced authorized source read per session/range; move eligible pure bounded reads to WAL readers after exact authorization ownership is preserved. |
| <a id="ar-140"></a>AR-140 | Read-only Store ownership | **Open bottleneck with donor in tree.** Most ordinary reads still enter the single kernel writer queue. `status_reader` already proves an existing query-only WAL connection can revalidate principals and read committed state while the writer transaction is held. | Route selected pure bounded reads through a small retained WAL reader owner; do not create a connection per request or a second database authority. |
| <a id="ar-141"></a>AR-141 | Live profile authorization and task projection | **Preserve.** Profile discovery/calls re-read Store authorization rather than caching grants; restricted mutations require caller-known IDs; failed optional task projection cannot retroactively fail a committed mutation; `tasks/cancel` uses ordinary queued-only `operation.cancel` and rereads terminal races. | Keep these boundaries while simplifying catalog/products and exact reads. |

<a id="trace-2026-10-24-ipc-mcp"></a>
## Source trace at 053f1e0: IPC, MCP and hot reads

Inspected 2026-10-24. This is source evidence, not an executed concurrent MCP/load
qualification.

### AR-127–130 — transport multiplexing already exists and is correct enough to retain

`swarm-client::Client` wraps an `Arc<Session>`. Clones share the same physical stream,
fixed reader/writer tasks, semaphore, pending-response map and connected/failure state.
`request_shared(&self, ...)` submits one correlated request. Tests cover out-of-order
replies, bounded capacity, pre-write cancellation, post-write cancellation, application
refusal, malformed/mismatched replies, partial EOF and writer backpressure.

The critical law is conservative:

```text
not submitted -> safe refusal/cancellation
possibly written + no valid exact reply -> OUTCOME_UNKNOWN + poison link
later command may reconnect
original command is never replayed
```

Kernel `ipc::serve_connection` independently supports concurrency. It authenticates the
first hello, then admits up to `max_inflight_per_connection` request tasks and funnels
responses through one bounded writer. When the peer disappears it drains already
accepted Store calls before recording disconnect. Transport loss therefore never means
an admitted application mutation failed.

### AR-131–133 — MCP serializes the multiplexed client

`McpFacade` stores `client: Mutex<Option<Client>>`; subscriptions use a separate
`pump_client` with the same shape. `request_on` acquires the mutex, connects if empty,
keeps the guard across `connected.request(...).await`, and clears the slot for selected
transport codes. Concurrent tool calls therefore queue behind the facade guard even
though the underlying Client and host can correlate them safely.

The connected fix is narrow:

```text
lock slot
-> connect only when absent/disconnected
-> clone shared Client
-> unlock
-> request_shared
-> on transport-class failure, relock and clear only if the slot is still disconnected
```

No failed request is retried. A later command reconnects. The post-failure recheck is
required: otherwise an old call can finish late and clear a replacement client created
by another call. Keep tool and pump links separate because they isolate responsibilities;
do not create one link per subscription.

### AR-134–136 — subscription bounds are useful, polling topology is not

Each subscription currently owns two tasks and a queue. The poller reads
`report.delta` pages on a 250 ms cadence, filters categories and emits committed
notifications. Queue capacity is bounded by count and aggregate serialized bytes.
When full, it counts dropped matching entries to an immutable cut and emits one lag
marker before newer entries. Failed reads retain the last fully examined cursor.
Unsubscribe/transport death affects no Store work.

Preserve those delivery semantics. Replace the topology with one session pump that:

1. maintains one examined Store cursor;
2. reads a bounded committed range once;
3. computes category matches once;
4. fans out to each subscriber's independent bounded queue;
5. records lag per subscriber;
6. coalesces idle wakes rather than polling N times.

Concilium/coordination categories are transition debt, not retained architecture. Their
removal follows the mailbox/coordination vertical cuts.

### AR-137–140 — exact reads are implemented as fleet reports on the writer

MCP `project_task` reads one exact Operation. For every nonterminal Operation with a
binding, `pending_inputs` then loops through every page of `report.attention` at 200
items/page and filters the assembled items by binding/generation. Profile policy even
requires `report.attention` for `tasks/get`. There is no current exact public pending
request read.

`report.attention` itself builds the current attention item collection before the page
is projected. Thus a small requested page does not bound source work. `report.delta`
is better bounded at the candidate scan, but it still walks the shared Observation
stream, performs grant resolution and projection on the single Store owner. Independent
subscription pollers repeat that work.

Most ordinary reads enter `Store::run`, the same kernel queue that owns SQLite writes.
The existing `status_reader` is the local donor: one long-lived read-only/query-only WAL
connection, deferred transactions, principal revalidation, and committed-snapshot reads
while the writer is held. Reuse the mechanism selectively. Do not move mutations,
provider/file/native I/O or ambiguous reads; do not create a connection per frontend
request.

### AR-141 — authorization and mutation truth stay in Store

Profile exposure narrows presentation but Store authorization is reread for catalog
list/search/calls. Restricted profiles require caller-known mutation request IDs.
When a mutation returns an Operation and optional MCP Task projection subsequently
fails, the facade returns the original successful result rather than inventing a failed
mutation. Tasks cancellation calls the ordinary queued-only cancellation and rereads a
race to terminal state.

These are retained boundaries. Removing the outer mutex or moving a pure read to WAL
must not cache grants, manufacture a retry or create a second Task state machine.

## Inspected source boundaries

| Source | Read boundary |
| --- | --- |
| `swarm-client/src/{client,multiplex}.rs` | Authentication, clone/shared session, request IDs, capacity, writer/reader tasks, cancellation and poisoning. |
| `swarm-kernel-host/src/ipc.rs` | Authenticated concurrent intake, response writer and disconnect draining. |
| `swarm-mcp/src/mcp/mod.rs` | Tool/pump slots, full-lock request path, live authorization and MCP Tasks projection. |
| `swarm-mcp/src/mcp/subscriptions.rs` | Per-subscription tasks/queues, report-delta polling, lag marker and category/resync products. |
| `store/object_reads.rs` | Global Observation timeline, bounded candidate scan and per-item grant projection. |
| `store/capacity.rs` | Current attention assembly and report projection. |
| `store/mod.rs` | Writer-queue routing for ordinary reads/mutations and separate special owners. |
| `store/status_reader.rs` | Query-only WAL reader, principal revalidation and committed-snapshot behavior. |
| MCP profile/catalog policy | Live method authorization, request-ID discipline and subscription requirements. |

Selected paths, not whole-project certification. Concurrent transport/load, reconnect
races, slow consumer behavior and writer fairness remain unexecuted.

## Next work

Continue the local Governor observation chain named in [Resume here](#resume-here).
Runtime IPC work is owned by [IPC/MCP](ipc-mcp.md): land the lock-release slice first,
then exact pending-input reads, one coalesced pump and bounded WAL read routing. Do not
combine those responsibilities into a new broker or frontend state store.
