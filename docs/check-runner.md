# Fixed-source CheckRunner

C06 implementation slice, 2026-10-01. The exact build result is recorded in the main README after CI. This file describes the implemented path, not a claim of live Muse/Windows qualification.

## One concrete path

`source.capture → task.submit → check.run → task.accept`

The order of submit/check may be reversed; both must refer to the same source candidate. Source capture reads **one exact local Git commit**, including its tree/blob identities, executable bits and hashes. It does not fetch, switch branches, stage files, run repository hooks or create worktrees. Uncommitted edits are not part of that candidate. The frozen manifest and its file bodies stay under the controller's own state directory.

`check.run` chooses an operator-configured profile; the requesting manager does not submit arbitrary argv or a fabricated verdict. It persists CheckRun/Operation, reserves the profile's target resource, and only then starts the same `swarm` binary as a transient check worker. That worker registers its identity before receiving the persisted go-ahead. There is no additional permanent service or database.

The worker copies the captured normal files into its own source directory, verifies them, runs the command, waits for its owned process group, retains stdout/stderr, verifies source again, and publishes a result. A successful process exit alone is insufficient for Cargo: its build-finished record and all configured target names must be observed without parsing/coverage gaps. Non-JSON output remains retained. No second model judges each command.

## Configure and invoke

Start the host with an explicit check configuration, such as [checks.example.toml](../config/checks.example.toml). Its target names are for this repository; edit the trusted profile for another project. The normal default configuration leaves checks disabled. `swarm check profiles` shows the loaded profiles and revisions.

Create/claim a Task as usual. For required machine evidence, start from [task-checked.example.json](../config/task-checked.example.json), not the review-only Task. Commit the work to main, then fill [source-capture.example.json](../config/source-capture.example.json) with the owned Attempt, revision, local repository path and exact full commit SHA.

```powershell
swarm --request-id capture-1 source capture --file capture.json
# Inspect operation.get with the returned operation_id; retain result.candidate_ref.
# Submit that candidate using the existing submission format and exact Task requirements.
swarm --request-id submit-1 task submit --file submission.json
swarm --request-id check-1 check run --file check.json
swarm check get CHECK_ID
```

Use normal `--config`, `--data-dir` and `--credential` arguments. [check-run.example.json](../config/check-run.example.json) selects the same candidate/profile revision. Read the original Operation for the final result reference. `artifact.get/read/export` exposes the machine report and its stdout/stderr references. Output ranges verify only touched log segments; exporting checks the whole digest. Full diagnostics remain on disk; small previews and target coverage keep reports cheap.

As a separate decision owner, include that CheckRun ID in the existing acceptance JSON. The actual record, exact candidate, profile revision, complete coverage and persisted execution receipt are checked. Human/model semantic review remains separate. Passing Clippy does not automatically accept a Task, publish code or close an Issue.

`check.cancel CHECK_ID --reason TEXT` cancels a still-queued check. Once execution has started, this slice does not implement active cancellation. Neither cancellation nor acceptance kills a native agent. Repeating a running check for the same Attempt/candidate/profile returns its original handle. Finished-cache reuse and automatic reverse-dependency scope are not implemented; the warm Cargo target is reused normally.

## Processes, resources and recovery

Windows workers assign **themselves** to a Job before starting a tool. Children inherit it; active-process accounting is used instead of waiting for a notification or just the parent PID. The worker is the last remaining member before the resource is released. Kill-on-close belongs only to that transient worker's Job, never to a Muse/Codex server. Linux workers use an independent process group and inspect non-zombie members. These are trusted verifier execution boundaries, not sandboxes against tools deliberately escaping through WMI, setsid or external daemons.

The host may disconnect or restart while a worker continues. Identity/go-ahead and the completion report are retained, and a new host collects the same result rather than re-running the command. Ordinary host shutdown drains IPC and stops admissions; it does not terminate an admitted worker. The host and worker both use the same state directory and executable deployment.

A worker crash without completion, missing launch evidence or a damaged report is **not** a pass or permission to reuse its target directory. It remains reconciling/unknown with a deduplicated incident. A lost worker whose lock is released does not occupy every unrelated scheduler slot. Automatic disposition of orphaned work after a worker/OS crash and active cancellation remain unfinished. Do not delete a resource record or replay its process because a timestamp is old.

The worker uses separate file handles for stdout/stderr; no parent-EOF assumption or model-stream timeout finishes the check. Top-level Windows launches suppress console windows, but native Windows launch and all descendant console behavior still require qualification on that platform.

## Explicit scope

The initial exporter accepts self-contained UTF-8-named regular Git files. Symlinks, gitlinks/submodules, LFS pointers, case collisions and unsafe Windows paths are rejected explicitly rather than omitted. External build inputs, private Cargo configuration and tools must be represented by the selected profile/environment. The example clears rustc wrappers **for the check process only**, without changing global Cargo settings. Inherited credential environment is not copied wholesale: a small runtime environment plus explicitly selected names is used. Do not store secrets in literal profile values.

No cache eligibility is claimed for changing external environments. SHA-256 demonstrates retained byte identity, not that the reviewer or configured executable is honest. The checker does not parse or obey instructions in the captured files; configured compilers/build scripts can nevertheless execute with that user's permissions.

All retained check data uses the existing `artifacts`, `operations`, `check_runs`, `observations` and `incidents` tables. Migration unchanged. Only `libc`, already present in Cargo.lock, is added as a direct Linux dependency for process-group setup; Windows uses existing windows-sys APIs.

Technical references read for this slice: [Cargo JSON messages](https://doc.rust-lang.org/cargo/reference/external-tools.html), [Windows Job accounting and inheritance](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects), [Git batch object reads](https://git-scm.com/docs/git-cat-file), [Git tree listing](https://git-scm.com/docs/git-ls-tree). These describe platform contracts, not completed runtime qualification.
