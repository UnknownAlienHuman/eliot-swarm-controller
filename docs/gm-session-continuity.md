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

## Reconnect or appoint a successor

For a new chat using the same manager identity, start its MCP connection with the
saved credential and the existing controller data directory. Read current state
before sending new work; no handover is needed for a changed transport link.

For a different manager identity, the local operator can use the existing CLI:

```powershell
swarm --data-dir C:\SwarmState --request-id register-successor client-create GM-next --role manager --out C:\SwarmState\GM-next.credential.json
swarm --data-dir C:\SwarmState --request-id designate-successor gm-handover GM-next
swarm --data-dir C:\SwarmState --credential C:\SwarmState\GM-next.credential.json status
swarm --data-dir C:\SwarmState --credential C:\SwarmState\GM-next.credential.json task list
```

Use the actual existing data directory. The operator's credential is local;
neither command needs a response from the previous GM chat. The successor then
uses its own manager credential for continuation. Retain each mutation's
`client_request_id` when a response is lost.

Former-owner automation read parameters are explicit:

```json
{
  "project_id": "project-a",
  "owner_manager_id": "GM-previous",
  "automation_id": "submission-audit"
}
```

Pass these to `automation.config.explain`; omit `automation_id` for
`automation.config.get`. This read does not enable, transfer or duplicate an
automation entry. Owner transfer remains a separate management operation.

The source now implements this continuity requirement. Verification requires a
real Store regression with an old GM, a successor using a different
client identity, preserved Attempt ownership and dispatch identity, positive
successor continuation and a denied unrelated manager. Native qualification is
recorded separately in [implementation status](implementation-status.md).

The local submission writer also separates admission from completion: a GM
change during immutable artifact publication cannot discard the verified file.
Completion retains the original submitting actor; a changed Task scope keeps
the artifact as history without applying it to the current Attempt. A submission
left `outcome_unknown` by a controller host crash has an explicit
[`task.submit.recover` readback path](gm-submission-recovery.md). The current GM
or local Operator verifies the exact already-existing file; recovery preserves
the original submitter and cannot publish a missing file. This is distinct from
losing the GM chat while the controller host remains running.
