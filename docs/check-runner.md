# Fixed-source CheckRunner

C06 implementation, updated 2026-10-01. Exact build/runtime evidence is recorded in the main README after verification. This guide describes the implemented path, not qualification of live Muse or the owner's Windows machine.

## One concrete path

`source.capture → task.submit → check.run → task.accept`

Submit/check may be reversed; both refer to the same source candidate. Capture reads one exact local Git commit, including tree/blob identities, executable bits and hashes. It does not fetch, switch branches, stage files, run repository hooks or create worktrees. Uncommitted edits are not included. The frozen manifest and bodies stay in the controller's state directory.

`check.run` selects a trusted configured profile, not arbitrary worker-supplied argv or verdicts. It persists CheckRun/Operation, reserves the target resource, then starts a transient process of the same `swarm` binary. The worker registers its identity before receiving the persisted go-ahead. No extra permanent service or database.

The worker materializes captured files, verifies them, executes the command, waits for its owned process group, retains stdout/stderr, verifies source again and publishes a result. Cargo requires build-finished and all configured target names without parsing/coverage gaps. Exit zero alone is insufficient. Non-JSON output is retained; no model judges every command.

## Configure and invoke

Use [checks.example.toml](../config/checks.example.toml). Its target names are for this repository; adjust the trusted profile for another project. Normal configuration leaves checks disabled. `swarm check profiles` lists loaded profiles/revisions.

Create/claim a Task. Required machine evidence uses [task-checked.example.json](../config/task-checked.example.json). Commit the work to main, then fill [source-capture.example.json](../config/source-capture.example.json) with Attempt, revision, absolute local repository and exact full commit SHA.

```powershell
swarm --request-id capture-1 source capture --file capture.json
# Read operation.get and retain result.candidate_ref.
swarm --request-id submit-1 task submit --file submission.json
swarm --request-id check-1 check run --file check.json
swarm check get CHECK_ID
```

Supply normal config/data-dir/credential options. [check-run.example.json](../config/check-run.example.json) names the same candidate/profile. The original Operation holds the final result reference; artifact.get/read/export exposes the machine report and its output references. Ranges verify touched log segments; export verifies the whole digest. Full diagnostics remain on disk, compact summaries in reports/mailbox.

A separate decision owner includes the actual CheckRun in the acceptance JSON. Record identity, candidate, profile revision, coverage and execution receipt are checked. Passing Clippy does not itself accept a Task, publish code or close an Issue. Active repeats coalesce within one Attempt; completed-cache reuse and automatic reverse-dependency scope remain pending.

## Cancellation: request, effect and release are separate

```powershell
swarm --request-id cancel-check-1 check cancel CHECK_ID --reason 'Superseded verification'
swarm check get CHECK_ID
```

Owner/operator permission is checked before accepting the request. The reply confirms durable cancellation intent, not process termination. Repeating that logical request returns its receipt; a second request for the same still-active check coalesces to the first cancellation operation. `check.get` exposes `cancel_request` and the terminal `cancellation` evidence. Already terminal checks keep their result.

Queued work is cancelled without running the tool, even when new-work admission is disabled. For a running control-version-2 worker, the supervisor delivers a token/CheckRun-addressed cancellation file. Delivery survives host restart and is idempotent. The worker observes it while waiting for go-ahead, before command spawn, while the main tool runs and while its descendants remain. Source materialization and result sealing are finite local operations, not interruptible file copies.

Active cancellation explicitly requests termination of this check's tools. It does not kill its reporting worker, host, other checks, or any native-agent family. Windows checks exact Job membership on pinned process handles before TerminateProcess. Linux uses pidfd_open/pidfd_send_signal with membership/birth checks, not kill-by-recycled-PID; Linux 5.3+ with available pidfd syscalls is required for this path. Termination is not a graceful application shutdown. It is never triggered merely by elapsed time, CPU load or host disconnect.

The worker waits for actual group emptiness before sealing output/releasing the target. A failed termination request is retained in the cancellation evidence; the target is not freed just because a signal was requested. A request observed during unfinished execution yields cancelled after the group ends; a request arriving only after execution finished does not rewrite its natural verdict. This is not an atomic transaction with the final OS instruction of the tool.

A pre-existing independently running older binary cannot be hot-upgraded. Its observed control version yields CHECK_CANCEL_UNSUPPORTED instead of pretending cancellation succeeded. Normal completion remains collectable. New workers advertise control_version=2; no database migration is required.

## Process ownership and recovery

The worker owns its Job/process group before launching tools. Windows uses a uniquely named Global Job, so a different logon session cannot mistake a session-local name's absence for termination. No existing Job is adopted. Children inherit membership; active accounting, not a single PID/notification, proves emptiness. Kill-on-close belongs only to this transient worker's Job. Linux uses an independent process group and boot/start identities; pidfd readiness distinguishes an exited main thread from a process with live threads. These are trusted execution boundaries, not sandboxes against tools deliberately escaping via WMI, setsid or external daemons.

A host disconnect/restart does not close an admitted worker. The next host collects its retained result, not a second execution. Ordinary host shutdown stops admissions/IPC, not working check processes.

After a worker crash, recovery acquires its released lock, rechecks completion, verifies retained worker identity, and observes that the original worker and its group are gone. It never terminates or re-runs orphaned processes. A live/unknown group retains only that target resource, while unrelated resources continue. Windows opens the exact recorded named Job read-only; a legacy unnamed Job remains unsupported for automatic orphan recovery. Linux requires the original boot/group identity and no live members; ambiguous PID/group reuse retains ownership conservatively.

New workers prepare `terminal.json` after publishing verified artifacts and before final completion. If the worker dies in that final gap, recovery validates and publishes the same receipt rather than discarding a completed result. Without a terminal receipt, an observed-empty group produces **incomplete**, unknown exit/coverage and retained stdout/stderr, never a guessed pass or cancellation. No replacement writer starts automatically. The terminal Store transaction resolves related check incidents and sends the normal single owner result.

Missing launch/worker identity, denied OS inspection, damaged control/artifact data and still-live orphans remain explicit recovery boundaries. Timestamp age and a free lock alone do not authorize resource release. Do not delete records or invent a completion to clear these cases. Existing binary deployments are kept while their workers run.

## Explicit scope and references

Initial capture supports self-contained UTF-8-named regular Git files. Symlinks, gitlinks/submodules, LFS pointers, case collisions and unsafe Windows paths are rejected, not omitted. External build inputs/configuration belong in the selected profile/environment. The example clears rustc wrappers for the check only, not global Cargo configuration. A small runtime environment and explicitly selected names are inherited; secrets must not be placed in literal persisted profile values.

No new table, package version, test framework or permanent service is required for cancellation/recovery. Existing artifacts/operations/check_runs/observations/incidents remain the authority. SHA-256 proves byte identity, not the honesty of configured executables or reviewers. Windows launch, complete descendant-console behavior, Cargo execution and load require their own qualification; compilation is not a substitute.

Technical sources read 2026-10-01: [Cargo JSON messages](https://doc.rust-lang.org/cargo/reference/external-tools.html), [Windows Job lifetime/accounting](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects), [Job membership](https://learn.microsoft.com/windows/win32/api/jobapi/nf-jobapi-isprocessinjob), [Job PID list](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_process_id_list), [kernel namespaces](https://learn.microsoft.com/en-us/windows/win32/termserv/kernel-object-namespaces), [pidfd_open](https://man7.org/linux/man-pages/man2/pidfd_open.2.html), [pidfd_send_signal](https://man7.org/linux/man-pages/man2/pidfd_send_signal.2.html), [Git batch objects](https://git-scm.com/docs/git-cat-file). These describe platform contracts, not completed product runtime qualification.
