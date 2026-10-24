# IPC and MCP — multiplexed transport, exact reads and one wake pump

This document defines the target frontend/Store boundary.
[Implementation H/J](implementation.md#h--separate-protocols-and-authorizers) owns
source changes; [current audit](audit-current.md#findings-ar-127141) records evidence;
and [status](../implementation-status.md) records landed behavior.

The target reuses the current authenticated multiplexed IPC. It does not add a broker,
frontend database, retry queue or second Task/event authority.

## 1. Product law

```text
one authenticated physical IPC link per responsibility
-> many bounded ID-correlated requests
-> no outer lock across an RPC
-> Store remains application authority
-> notifications are coalesced hints
-> exact Store reads recover truth
```

Tool calls and the subscription pump remain separate responsibilities. Multiple tool
calls can share one multiplexed tool link. All subscriptions in one MCP session share
one delta/wake pump. A transport timeout/disconnect never authorizes replay of an
ambiguous mutation.

## 2. Retained transport contract

### Client

`swarm-client::Client` clones share one `Session`:

- one authenticated socket/pipe;
- one reader task and one writer task;
- one bounded in-flight semaphore;
- one pending-response map;
- one connection/failure state;
- unique connection-scoped request IDs.

`client.hello` completes before a public Client is returned. Application frames cannot
overtake authentication. `request_shared(&self, ...)` is the concurrent call surface.

### Outcome classes

| Point of failure | Result |
| --- | --- |
| Capacity unavailable before queue admission | `IPC_CAPACITY`; no frame sent. |
| Request future cancelled before writer sends | no frame sent; connection remains usable. |
| Application returns a valid JSON-RPC error | exact application error; connection remains usable. |
| Request may have been written but exact reply is absent/invalid | `OUTCOME_UNKNOWN`; poison link and all unresolved calls. |
| Link already poisoned/closed | `DISCONNECTED`; no frame sent. |

No transport path resends an application request. A later command may establish a new
link, but the original mutation is reconciled through its caller-known request/Operation
identity.

### Host

The host authenticates hello first, then accepts up to the configured in-flight bound.
Request tasks may complete out of order; one response writer preserves bounded frame
output. On peer disappearance, already accepted Store calls are drained before the
connection is retired. Disconnect is transport evidence, not application cancellation.

## 3. MCP connection slots

Each MCP session owns:

```text
tool_client: one optional shared Client
pump_client: one optional shared Client
```

A slot mutex protects only connect/replace/clear of the optional handle. It never spans
an application RPC.

Target request algorithm:

```text
lock slot
if absent or disconnected:
    connect and authenticate one Client
clone Client
unlock slot
result = client.request_shared(method, params)
if transport-class failure:
    lock slot
    clear only when the currently stored Client is still disconnected
    unlock slot
return result
```

The post-failure recheck prevents an old late failure from clearing a healthy replacement
installed by another request. It does not retry the failed command.

Do not reconnect on application errors such as `FORBIDDEN`, `INVALID_PARAMS`,
`NOT_FOUND`, `CONFLICT` or `IPC_CAPACITY`. The shared Client itself determines whether
the transport remains connected.

## 4. MCP profile and authorization boundary

A frontend profile narrows presentation only. Store authorization remains authoritative
for every forwarded call.

Preserve:

- profile name and expected client identity fixed for one stdio process;
- catalog list/search/call reads current Store authorization rather than a session cache;
- participant/reviewer access remains exact object scope;
- restricted-profile mutations require a caller-owned `client_request_id` before
  dispatch;
- a transport failure never generates a new request ID or retries;
- a tool appearing in the catalog grants no Store action;
- public MCP contains no generic JSON passthrough.

A live authorization read may itself run concurrently with another tool call over the
same multiplexed tool link. Store still orders domain mutations where required.

## 5. MCP Tasks projection

The durable Operation remains the sole MCP Task state. The facade stores no task
lifecycle.

### `tasks/get`

Target path:

```text
operation.get(exact Operation)
+ operation.pending_input(exact Operation/binding generation)
-> MCP DetailedTask
```

The exact pending-input read returns only current requests attributable to that
Operation/binding generation under the caller's current/historical grant. It must not
page global `report.attention` and filter in the frontend.

Terminal mapping remains:

- settled → completed with the Operation result;
- rejected → failed with exact Eliot error in data;
- cancelled → cancelled;
- nonterminal with exact pending input → input required;
- other nonterminal/unknown → working with honest Store state.

### Mutation result to Task seed

If an admitted mutation returns an Operation and optional Task projection subsequently
fails, return the original successful mutation result. Projection failure never rewrites
the durable outcome or invites replay.

### `tasks/cancel`

Use ordinary `operation.cancel` for a queued Operation only. Already-sent work requires
native cancellation/reconciliation. A race to terminal is reread and acknowledged; no
second cancellation product exists.

## 6. Exact hot reads

Replace broad report reduction in common exact paths.

| Consumer need | Target read |
| --- | --- |
| One Operation | indexed `operation.get` plus exact current projection cards genuinely needed by that Operation. |
| Pending native request for one Operation | exact Operation/binding-generation lookup. |
| One mailbox page | recipient-indexed mailbox rows. |
| One Task/Attempt | exact primary-key/object-scope read. |
| One review/candidate | exact assigned review/evidence read. |
| Fleet attention/report | explicitly bounded report projection, never a hidden dependency of exact reads. |

Pagination must bound Store work, not only response size. Do not build the full attention
vector before slicing a page. Use indexed current subjects and keyset continuation where
a report genuinely needs fleet scope.

## 7. Read ownership and WAL routing

The single writer remains the authority for mutations and any read that must be coupled
to an immediate state change. Pure bounded reads may use a retained WAL reader owner.

The existing status reader is the donor:

- one long-lived read-only/query-only SQLite connection;
- bounded request queue;
- deferred read transaction per request;
- current principal revalidation;
- committed snapshot visibility while the writer is held;
- no migration or repair.

Target evolution:

1. classify methods as writer-coupled, pure bounded WAL read or external/adapter path;
2. move one connected pure read at a time;
3. use the same domain authorization and projection implementation or a shared pure
   owner, not two policy copies;
4. retain query/index bounds and explicit partial/gap output;
5. keep one/small fixed reader ownership, not a connection per request;
6. measure writer occupancy and fairness before enlarging the reader set.

Initial candidates are exact Operation/Task/Attempt/pending-input/mailbox reads after
their schemas and authorization are connected. Provider, filesystem, Git, network and
native I/O never run inside a SQLite read transaction.

## 8. One session subscription pump

Subscriptions are freshness hints over committed Store facts. Store history and exact
object reads are replay authority.

Target session topology:

```text
one pump Client
one examined Store cursor
one bounded delta read loop / wake loop
N subscriber registrations with category filters and independent bounded queues
N transport forwarders only where required by the MCP library
```

The pump reads each committed range once, projects category membership once and fans out
to subscribers. It does not perform one `report.delta` scan every 250 ms per
subscription.

A Store-change wake may replace fixed-rate polling when connected. Wakes coalesce; a
lost wake causes a bounded delta rescan. A slow subscriber never slows the pump or
another subscriber.

## 9. Subscriber bounds and lag

Preserve current useful semantics per subscriber:

- bounded item count;
- bounded serialized bytes, including the in-flight notification;
- cursor advances only for delivered or explicitly counted-as-dropped entries;
- queue overflow creates one lag episode;
- lag marker names dropped matching count and `(from_cursor, through_cursor]`;
- lag marker precedes newer delivered entries;
- failed page read does not advance the cursor;
- unsubscribe/transport death stops only that session subscription;
- reconnect creates new subscription state from a caller-supplied Store cursor.

After mailbox/coordination migration, retained categories are only those backed by
current products, such as report invalidation, recipient mailbox wake and exact
Operation/current-object changes. Remove Concilium/coordination categories and their
recovery-method requirements with those products.

## 10. Report delta

`report.delta` may remain a bounded committed-fact source for the session pump. It is not
an event database owned by MCP.

Requirements:

- one finite cut/cursor and explicit scan coverage;
- bounded candidate rows/bytes/time;
- exact principal authorization revision for the cut;
- per-item disclosure through retained object grants;
- explicit damage/gap references rather than silent omission;
- no native/provider/filesystem/network call;
- no duplicate poller scans for the same session/range.

Once recipient mailbox and current observation sources have exact tables, the pump may
prefer addressed invalidation rows instead of a global Observation scan. Do not create a
second durable frontend log.

## 11. Implementation slices

### J0 — release MCP slot lock

- connect/clone under the slot mutex;
- call `request_shared` after unlocking;
- generation-safe disconnected-slot clearing;
- preserve separate tool/pump slots and no retry;
- focused concurrent out-of-order, application-error, disconnect and late-failure cases;
- minimal `swarm-mcp` format/compile/Clippy.

### J1 — exact pending input

- add one Store-authorized exact read for an Operation/binding generation;
- switch `tasks/get` to it;
- remove `report.attention` from the task-get profile requirement;
- preserve input-required mapping and exact `agent.reply` address;
- prove unrelated fleet attention cannot affect result/cost.

### J2 — bounded attention/report production

- replace full attention materialization with bounded indexed production/keyset scan;
- retain explicit coverage/gaps and stable ordering;
- keep fleet reports explicit, not hidden dependencies.

### J3 — one subscription pump

- one session cursor/read loop;
- fanout to independent bounded queues;
- preserve lag episodes and recovery reads;
- remove per-subscription poller tasks and redundant page reads;
- remove coordination/Concilium categories with their product cuts.

### J4 — WAL read migration

- generalize the retained status-reader mechanism only as needed;
- move exact pure reads one by one;
- keep authorization singular and current;
- measure writer occupancy/fairness;
- never migrate mutations or external I/O.

### J5 — surface cleanup

- remove duplicate frontend requirements and report dependencies;
- keep catalog/profile tables synchronized with actual Store handlers in the same slice;
- remove compatibility methods with their owners, not aliases.

## 12. Failure semantics

| Event | Required behavior |
| --- | --- |
| Application error on healthy link | Return exact error; keep link. |
| IPC capacity refusal | Return pre-submit refusal; keep link. |
| Possible write + lost/invalid response | `OUTCOME_UNKNOWN`; poison shared link; no retry. |
| One concurrent request poisons link | All unresolved calls become unknown; later commands reconnect. |
| Old client fails after replacement exists | Do not clear healthy replacement. |
| Tool link fails | Pump link remains independent. |
| Pump link fails | Tool link remains independent; next pump read reconnects, no replay. |
| Subscription queue fills | Record one explicit lag episode; other subscribers proceed. |
| Delta read fails | Cursor stays at last examined fact; retry read later. |
| Exact WAL read sees revoked principal | Fail authorization; no stale grant cache. |
| Writer is busy | WAL read sees committed snapshot or bounded timeout according to its method contract. |

## 13. Verification

Minimum connected proofs:

- two concurrent MCP tool calls use one authenticated link and can complete out of order;
- one application refusal does not block/poison another concurrent call;
- cancellation before write sends no frame;
- possible-write disconnect marks every unresolved call unknown and sends no retry;
- late old-client failure cannot erase a healthy replacement;
- tool and pump links fail/reconnect independently;
- one session with many subscriptions reads each Store range once;
- slow subscriber receives one exact lag marker while a fast subscriber continues;
- `tasks/get` performs no global attention scan and returns only the Operation's exact
  pending inputs;
- report page size bounds Store work, not only serialized output;
- exact WAL read revalidates revocation and reads committed state while writer is held;
- mutation completion cannot be reversed by optional task/read projection failure;
- removed coordination categories leave no profile, schema, resync or compatibility
  route;
- no frontend cache, replay queue, task database or durable event log is introduced.

During construction run rustfmt and minimal affected warnings-denied compile/Clippy.
Focused concurrency, reconnect and slow-subscriber cases close each slice. Broad
multi-client/load/installed qualification follows the connected product path.
