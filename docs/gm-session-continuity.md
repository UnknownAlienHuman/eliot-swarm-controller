# GM continuity after chat loss

The project belongs to the persistent controller Store, not to a particular GM
chat. Tasks, Attempts, candidates, Operations, workspace leases and automation
cursors must remain usable after that chat disappears.

An authenticated connection has an ephemeral `link_id`. Its credential names a
durable `client_id`. A fresh chat using the same saved credential reconnects to
that identity; it does not create a new Task, Attempt or native start. Changing
the optional GM native binding for the same client does not rotate GM authority
or invalidate that client's queued publication. The GM epoch changes when the
designated client changes.

If the old GM is unavailable, the local operator can designate a registered
successor with `gm.handover`. The old chat's participation is unnecessary. The
designated successor must be able to read retained project work and explicitly
continue the exact current Attempt, including its submission, assigned review,
checks and supported native binding commands. The original owner, actual caller,
candidate and producer lineage remain recorded. Continuation authority does not
require rewriting those historical identities.

Recovery reads the authoritative `task.list`, `task.get`, `attempt.get`,
`operation.list`, `operation.get`, `swarm.queue.get` and `report.attention`
projections. `report.delta` supplies committed changes. A successor can inspect
the former manager's automation configuration and explanation with explicit
`owner_manager_id`; this selects retained state rather than copying entries or
resetting cursors. Historical directed mailbox entries retain their recipients.

An existing initial dispatch returns its retained Operation rather than starting
the Task again. Sent or uncertain effects are resolved through their existing
readback path. A successor's fresh request is checked against the current Task,
Attempt, binding generation and candidate. Chat recovery does not imply native
bridge restart; `agent.recover` retains its separate runtime recovery contract.

The continuity implementation is being updated to this requirement. Acceptance
requires a real Store regression with an old GM, a successor using a different
client identity, preserved Attempt ownership and dispatch identity, positive
successor continuation and a denied unrelated manager. Native qualification is
recorded separately in [implementation status](implementation-status.md).
