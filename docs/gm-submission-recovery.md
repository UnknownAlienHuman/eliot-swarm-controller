# Recovering a submission after a host crash

`task.submit` first records an immutable submission document, then publishes its
deterministic artifact file, then commits the artifact and Task transition. If
the host stops between the filesystem and database steps, startup marks the
submission Operation `outcome_unknown`. The ordinary `task.submit` path does
not retry that Operation: an unknown submission needs explicit recovery.

The current GM or the local Operator can request recovery with the CLI:

```powershell
swarm task recover-submission <task.submit-operation-id>
```

The equivalent Store/MCP method is `task.submit.recover` with the exact target
`operation_id` and the usual mutation `client_request_id`. A target already
settled by another completion path returns its stored result without repeating
artifact registration. Use a new request ID for a later recovery attempt after
a prior recovery settled as held-unknown. The CLI and Store response report the
current recovery result; `operation.get` also exposes that durable result.
The initial admission receipt remains immutable and separate from completion.

Recovery retains the original submitter, Task, Attempt, candidate, prior
submission, and canonical `submission_document`. The Store reconstructs the
expected artifact record from that retained document and reads only its
deterministic path. It verifies the complete file length and digest; it never
creates or republishes the file. A file that is missing settles the recovery
Operation as `held_unknown` and leaves the original `task.submit` Operation in
`outcome_unknown`. Other filesystem errors are returned distinctly and leave
the target unknown so the read can be retried.

When the exact file exists and verifies, one database transaction registers the
artifact if needed and settles the original submit Operation. If the same Task
revision, unreleased Attempt, and expected prior submission are still current,
the result is `applied` and the Attempt points to the recovered submission.
If that scope has since changed, the verified artifact is retained as
`stale_submission_scope` history without changing the current Attempt. A
concurrent recovery that already settled the original Operation returns its
existing result and does not register another artifact or emit another
submission event.

Recovery requires current GM authority (or the local Operator) for the exact
Task and project. A Manager must still be registered, hold the GM designation,
and have current Task/project scope. Recovery is a filesystem readback and
database reconciliation only: it does not replay native work, impersonate the
original caller, accept the Task, or bypass acceptance checks.
