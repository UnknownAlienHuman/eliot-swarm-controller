# Host startup and failure readback

The controller records a lifecycle receipt in its existing Store after acquiring the exclusive data-directory lock. Startup records `starting`; binding the IPC endpoint records `running`. Graceful shutdown drains admitted work before recording `stopped`. A failed supervisor or startup records `failed` with a bounded controller error code. Diagnostic receipts contain no native response body, credential or raw error message.

`host.status.host_lifecycle` returns the current receipt, the last exit and the latest failure. A later graceful exit updates `last_exit` but retains `latest_failure`; it does not acknowledge or erase the earlier failure. Failure history is factual, and does not block normal work. After a new host acquires the same exclusive lock, a previous `starting` or `running` marker produces `HOST_INTERRUPTED`. This proves that the earlier host did not record its exit; the cause remains unknown. The receipt belongs to the controller's host epoch and survives loss of a manager chat. A malformed optional lifecycle receipt produces a safe diagnostic without hiding the rest of host status. Database failures remain errors.

IPC connection failure before dispatch returns `HOST_UNAVAILABLE`. An incomplete authentication handshake returns `HOST_HANDSHAKE_FAILED`. Both mean that no application request was sent on that connection. An application request whose response was lost remains `OUTCOME_UNKNOWN`; reconnect and read its original Operation before deciding whether to retry. A host failure does not authorize replay of admitted native work.

If startup fails before the Store is available, the foreground launching process receives a terminal error naming `data_root`, `credential_bootstrap` or `store_start` and the controller error code. A database startup failure cannot promise a receipt in that unavailable database. While the host is down, IPC reports unavailability; lifecycle history becomes readable after a successful restart.

Owned OpenCode startup failures are persisted separately with their exact launch/open Operation and binding references. The current GM or local Operator receives a safe `manager_action_required` projection through `operation.get` and `swarm.exceptions.get`. The original Operation admission result remains intact. A replacement GM can inspect the same durable failure without the former chat.

Implementation qualification is recorded in `docs/implementation-status.md`. These receipts do not introduce an automatic host restart, a model call or a new daemon.
