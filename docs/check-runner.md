# Fixed-source CheckRunner

C06 implementation, updated 2026-10-01. Exact build/runtime evidence is recorded in the main README after verification. This guide describes the implemented path, not qualification of live Muse or the owner's Windows machine.

## One concrete path

`source.capture → task.submit → check.run → task.accept`

Submit/check may be reversed; both refer to the same source candidate. Capture reads one exact local Git commit, including tree/blob identities, executable bits and hashes. It does not fetch, switch branches, stage files, run repository hooks or create worktrees. Uncommitted edits are not included. The frozen manifest and bodies stay in the controller's state directory.

`check.run` selects a trusted configured profile, not arbitrary worker-supplied argv or verdicts. It persists CheckRun/Operation, reserves the target resource, then starts a transient process of the same `swarm` binary. The worker registers its identity before receiving the persisted go-ahead. No extra permanent service or database.

The worker materializes captured files, verifies them, executes the command, waits for its owned process group, retains stdout/stderr, verifies source again and publishes a result. Cargo requires build-finished and all configured target names without parsing/coverage gaps. Exit zero alone is insufficient. Non-JSON output is retained; no model judges every command.

## Configure and invoke

Use [checks.example.toml](../config/checks.example.toml). Its target names are for this repository; adjust the trusted profile for another project. Normal configuration leaves checks disabled. `swarm check profiles` lists loaded profiles/revisions.

Create/claim a Task. Required machine evidence uses [task-checked.example.json](../config/task-checked.example.json). The candidate does not have to be `main`: capture reads one exact commit from any project or repository, and CheckRunner semantics attach to that exact commit, not to a branch name. Committing the work to `main` is a development rule of this repository, not a CheckRunner requirement. Fill [source-capture.example.json](../config/source-capture.example.json) with Attempt, revision, absolute local repository and exact full commit SHA.

```powershell
swarm --request-id capture-1 source capture --file capture.json
# Read operation.get and retain result.candidate_ref.
swarm --request-id submit-1 task submit --file submission.json
swarm --request-id check-1 check run --file check.json
swarm check get CHECK_ID
```

Supply normal config/data-dir/credential options. [check-run.example.json](../config/check-run.example.json) names the same candidate/profile. The original Operation holds the final result reference; artifact.get/read/export exposes the machine report and its output references. Ranges verify touched log segments; export verifies the whole digest. Full diagnostics remain on disk, compact summaries in reports/mailbox.

A separate decision owner includes the actual CheckRun in the acceptance JSON. Record identity, candidate, profile revision, coverage and execution receipt are checked. Passing Clippy does not itself accept a Task, publish code or close an Issue. Active repeats coalesce within one Attempt. When every resolved input is reusable, completed results may be admitted only from a directly matching original CheckRun; reverse-dependency scope is recorded on each run. Build and runtime qualification remains separate from this implementation contract.

## Effective build inputs

Before a run starts, CheckRunner resolves the candidate and any accepted baseline from verified source bytes, then records the effective argv, required targets, profile identity, executable and Rust toolchain hashes/version, platform/architecture, declared environment identity, versioned external-input digests, and scope plan. The reusable fingerprint includes candidate and optional baseline content digests; it excludes capture, artifact, Task, Attempt, CheckRun, Operation, and worker-token IDs. Built-in path/tool variables and explicitly classified `fingerprint_env` variables are represented by value digests. Other configured environment values are opaque: their values are neither hashed nor persisted, and their presence disables reuse. Runtime environment and executable hashes are checked again immediately before command spawn.

Metadata/version probes have a 30-second command deadline and bounded stdout/stderr. After timeout, output overflow, or normal command exit, the probe owner cancels leftover members and waits until its owned process group is observed empty before returning. Windows uses a Job Object; on Linux, group emptiness covers processes that remain in the owned process group, and a child that deliberately calls `setsid` or changes groups can escape that scope. The check executable and its configuration are trusted inputs; this mechanism does not claim host-wide containment of arbitrary descendants. The deadline bounds the external command; cleanup may take longer if the OS cannot promptly prove group emptiness, because the helper does not detach from a potentially live tool.

## Resource lease

A build target is held under an exclusive lease for the duration of a run: no second check writes to the same target concurrently. An active worker holds its target, and so does a worker whose disposition is unknown — a live or unknown-disposition group retains only that target resource while unrelated resources continue (see Process ownership and recovery). The lease is released only after the worker's group is known to be empty or its disposition is otherwise established; requesting termination, observing elapsed time or finding a free lock does not by itself release it.

For a `cargo_json` profile, trusted configuration can explicitly set uppercase
`CARGO_TARGET_DIR` in `[checks.profiles.environment]` to an existing absolute,
normalized directory. Both executors reject symlinks and Windows reparse points
in that path; they do not create its parents. Its value is hashed in the effective
input identity. An ambient or inherited `CARGO_TARGET_DIR` and a `--target-dir`
argument remain rejected. Profiles sharing that directory must use the same
`resource` value so their leases serialize writes to the cache. Without the
explicit setting, CheckRunner keeps its existing DataRoot per-resource target.

## Baseline, reverse scope and reuse

At Attempt claim, Store freezes the currently valid accepted candidate for the same project as the baseline. A missing or unproven baseline is not consumed and does not reject the run; it widens the plan to the configured whole-workspace command with an explicit reason. The baseline remains fixed for that Attempt even if another candidate is accepted later.

For Cargo JSON profiles using a confirmed Cargo executable, the resolver runs bounded, offline, locked Cargo metadata against each verified captured source. A complete member graph maps path dependencies by their canonical dependency manifest path to the exact captured workspace member. Missing, ambiguous or external path dependencies make the graph incomplete. With a valid baseline and complete matching graphs, changed package files select that package plus its reverse local-dependency closure. Shared Cargo configuration, lockfiles, toolchain selection, code generation inputs, graph changes, unmapped changes, or incomplete metadata widen to the whole workspace. Unknown graph evidence never becomes an empty graph. Any effective Cargo config not byte-matched to the captured `.cargo/config` files—including ancestor files, `CARGO_HOME` configuration and stale content-workspace files—or a captured config with an `include` makes the graph untrusted, widens scope, and disables reuse. Captured configs without includes are covered by the source digest; changes to them widen the scope.

The persisted `scope_plan` lists selected and excluded packages, changed paths, required/selected targets, content-only target owners, coverage gaps, graph identity and widening reasons. A required target excluded from a narrow plan is a coverage gap and cannot produce a passing result. Since the current profile format names Cargo targets by bare target name, a required name shared by multiple workspace targets is widened and reported as a coverage gap. Cargo JSON artifacts count only when their package ID resolves to the captured package manifest and their target kind and source path match the planned identity; a same-name dependency artifact cannot satisfy workspace coverage. Non-Cargo profiles use the whole configured command because CheckRunner cannot prove package-level scope.

Reuse is opt-in through trusted `reproducible = true` configuration and remains disabled whenever any effective input is unverified, opaque, or unversioned, including tool/version probes, Rust toolchain identity, Cargo offline/frozen behavior, or workspace graph. A reusable Cargo profile must also provide `versioned_inputs.build_environment`: a nonsecret, immutable image or toolchain-snapshot identity covering external build tools that CheckRunner does not discover individually, such as linkers, rustdoc, wrappers, and code-generation tools. Without it, reuse is disabled with `cargo_external_tools_unversioned`. The trusted profile owner attests that build scripts, procedural macros, services, time-dependent behavior, and any other inputs outside the resolver's captured identities are fixed by that environment or otherwise make the check non-reusable. This attestation does not override explicit failures such as an unversioned Cargo config, an unknown config include, network access, or missing graph evidence. `versioned_inputs` stores only digests of immutable external identities; it does not freeze mutable services or make network/time-dependent work reusable. Reuse verifies the original result document, output bytes, full source manifest, exact coverage and original process receipt, and current accepted-source provenance. Cache entries point directly to that original run; cache-to-cache chains are not admitted. Invalid, missing or stale evidence makes the candidate unusable.

A prior baseline failure is retained with its cause and source identity; it is not presented as a regression introduced by the candidate, and it does not excuse a new defect in a changed guarantee.

## Evidence publication and protected acceptance inputs

Evidence is published before a pass: the verified artifacts and execution receipt for a run are published and retrievable before any pass verdict for that run is relied on, and on timeout or worker loss the retained stdout/stderr and the incomplete receipt (unknown exit/coverage, never a guessed pass) are the published evidence (see Process ownership and recovery).

Acceptance inputs are protected. Check profiles are trusted configuration selected by the controller, not arbitrary writer-supplied argv or verdicts; the writer whose candidate is being checked does not edit the profile, its revision, or the protected test baseline that judge that candidate. Changing a profile or baseline is a separate trusted change, recorded with its own identity — it is not part of the candidate under check.

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

## Pre-identity launches

At spawn the host records its own launch receipt for the worker process (platform-pinned process instance: pid plus creation time/start ticks and boot identity), persisted in the CheckRun spec before it is relied on. If the worker dies before publishing `worker.json`, there is no admitted worker to recover and nothing to reconcile against. Only with that receipt, and only after the spawned process and its prospective group (the group the worker would have led) are proven departed, the launch is fixed terminal as **incomplete** with unknown exit/coverage and gap `worker_lost_before_identity`. Before identity publication the worker spawns nothing and the command never runs, so no tool output is being guessed and no command is replayed. Without a launch receipt — workers spawned by an older host — or while departure is unproven, the check stays held and only that target resource is retained. Nothing is inferred from the absence of a process, a file or a lock alone.

## Explicit scope and references

Initial capture supports self-contained UTF-8-named regular Git files. Symlinks, gitlinks/submodules, LFS pointers, case collisions and unsafe Windows paths are rejected, not omitted. External build inputs/configuration belong in the selected profile/environment. The example clears rustc wrappers for the check only, not global Cargo configuration. A small runtime environment and explicitly selected names are inherited; secrets must not be placed in literal persisted profile values.

No new table, package version, test framework or permanent service is required for cancellation/recovery. Existing artifacts/operations/check_runs/observations/incidents remain the authority. SHA-256 proves byte identity, not the honesty of configured executables or reviewers. Windows launch, complete descendant-console behavior, Cargo execution and load require their own qualification; compilation is not a substitute.

Technical sources read 2026-10-01: [Cargo JSON messages](https://doc.rust-lang.org/cargo/reference/external-tools.html), [Windows Job lifetime/accounting](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects), [Job membership](https://learn.microsoft.com/windows/win32/api/jobapi/nf-jobapi-isprocessinjob), [Job PID list](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_process_id_list), [kernel namespaces](https://learn.microsoft.com/en-us/windows/win32/termserv/kernel-object-namespaces), [pidfd_open](https://man7.org/linux/man-pages/man2/pidfd_open.2.html), [pidfd_send_signal](https://man7.org/linux/man-pages/man2/pidfd_send_signal.2.html), [Git batch objects](https://git-scm.com/docs/git-cat-file). These describe platform contracts, not completed product runtime qualification.
