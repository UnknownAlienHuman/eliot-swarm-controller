# Fixed-source CheckRunner

Current contract for `source.capture → check.run → task.accept`.
[Implementation K](agent-operations/implementation.md#k--selected-effect-owners) owns the
migration to one executor; [audit AR-065–070](agent-operations/audit-register.md#ar-065)
records the verified source boundaries. This document states working behavior and
explicit gaps, not installed-platform qualification.

## 1. Product boundary

CheckRunner proves machine evidence for one immutable candidate. It does not accept a
Task, judge every requirement, publish code, own a second workflow or execute
writer-supplied commands.

```text
exact Git commit
-> immutable source artifact
-> trusted CheckProfile + resolved inputs
-> one admitted CheckRun
-> verified process/parser/output evidence
-> optional strictly-proven reuse
-> common candidate-specific acceptance gate
```

Store owns Task, Attempt, Operation, profile selection, evidence identity and
acceptance. The check executor owns only one admitted command and its process family.

## 2. Fixed source capture

`source.capture` requires:

- exact Task revision and unreleased Attempt;
- an absolute configured repository;
- a full exact commit object ID;
- current workspace authority;
- one durable Operation/client request identity.

The capture reads Git objects for that commit. It does not fetch, checkout, switch
branches, stage files, execute hooks, create a worktree or include uncommitted edits.

Accepted files retain path, mode, blob identity, byte length and SHA-256. Only supported
regular files are included. Symlinks, gitlinks/submodules, Git LFS pointers, unsafe
Windows paths, case collisions and ambiguous names reject the capture; they are never
silently omitted. The complete manifest and all bytes are verified before Store settles
the Operation.

The artifact ID is deterministic from the original capture Operation. A started capture
may finish after Task revision/release because the sealed admission—not mutable current
ownership—authorizes that narrow completion.

### Current restart gap

Capture is still performed synchronously by the host request path. Host restart changes
a `sending` capture to `outcome_unknown`. Retrying the same exact request may rebuild the
same immutable commit artifact safely, but no current-state owner automatically resumes
or settles interrupted capture.

Target: one bounded reconciler reuses the original Operation/admission and either
adopts the already-published deterministic artifact or repeats the exact read-only
commit capture. Do not add a source workflow, mutable branch retry or synthetic
completion.

## 3. Trusted profile and resolved inputs

`check.run` names only:

```text
Attempt
candidate_ref
profile_id
profile_revision
client_request_id
```

The trusted local profile supplies executable, argv, parser, resource name,
environment, expected targets and reuse policy. A Manager cannot supply an arbitrary
command or verdict.

Before final reservation the host resolves and freezes:

- candidate and optional accepted baseline;
- executable and Rust/Cargo/toolchain identity;
- effective argv, environment and working directory;
- immutable/versioned external inputs;
- source and profile identity;
- package/dependency scope and expected parser targets;
- resource lease and optional standalone-executor pin;
- a complete input/cache fingerprint.

The final Immediate transaction rechecks the source facts. A changed candidate,
profile, tool, environment, baseline or scope invalidates the prepared plan.

### Cargo scope and effective inputs

At Attempt claim, Store freezes the currently valid accepted candidate for the same
project as baseline. Missing or unproven baseline widens the check; it is never treated
as an empty diff.

For `cargo_json`, the resolver runs bounded offline/locked metadata against the captured
source. It maps local path dependencies by exact captured manifest. Missing, ambiguous
or external path dependencies, untrusted Cargo configuration, unknown `include`,
graph changes, shared lock/toolchain/configuration changes or unmapped changed paths
widen to the whole workspace and disable reuse. A narrow plan records selected/excluded
packages, changed paths, graph identity, required targets and every widening/gap reason.

A bare required target name that matches several captured workspace targets is
ambiguous and cannot pass narrow coverage. Parsed artifacts count only when package ID,
manifest, target kind and source path match the captured planned target.

Built-in path/tool variables and explicitly classified fingerprint environment values
are represented by value digest. Other configured environment values remain opaque and
disable reuse. Runtime executable/environment identity is checked again before spawn.

An explicit uppercase `CARGO_TARGET_DIR` may point to an existing absolute,
non-reparse directory. Profiles sharing it must use the same resource lease. Ambient
target directories and command-line `--target-dir` are rejected.

## 4. One command, one process owner

The command does not start when `check.run` is merely queued.

```text
reserve CheckRun
-> select pending exact row
-> prepare private check directory
-> launch worker and retain launch identity
-> worker enters owned Job/process group
-> Store verifies exact worker identity
-> Store writes token-bound Go
-> worker resolves one-use plan
-> direct argv execution
```

Missing, stale or uncertain Go never starts the command. Execution uses no shell,
clears ambient environment, uses the admitted working directory and captures bounded
stdout/stderr files.

Cancellation is CheckRun/token-addressed. It records durable intent first, requests
termination only for the exact owned family, and waits for actual family emptiness.
A signal request, elapsed time, free lock or direct-child exit does not release the
resource.

Windows uses the owned Job/process identity; Linux uses the retained boot/process-group
identity and pidfd-based cancellation where required. These are custody boundaries, not
a sandbox against a deliberately escaping tool.

## 5. Recovery

Host restart converts `running` CheckRuns to `reconciling`. Recovery never executes the
command again.

- A retained terminal receipt is revalidated and published once.
- A worker lost after terminal preparation can complete from `terminal.json`.
- A proven-empty family without terminal evidence becomes `incomplete`, never pass.
- A live or uninspectable family holds only its exact resource.
- Pre-identity launch recovery requires the retained host launch receipt and proven
  departure of both prospective worker and group.
- Damaged or ambiguous custody remains held and visible.

Before worker identity is published, the worker has not received Go and cannot have
started the command. A persisted launch receipt plus proven process/group departure may
therefore settle that exact launch as incomplete. Missing launch identity remains held;
absence of a file, process or lock is not proof.

Unrelated resources continue. No replacement writer starts automatically.

## 6. Passing evidence

Exit zero is necessary but insufficient.

### `exit_code` parser

Passing coverage is exactly `process_exit` with no gaps.

### `cargo_json` parser

Passing requires:

- `build_finished=true`;
- zero parsed errors;
- exact expected target set;
- unique captured package/manifest/source/kind identity for every target;
- emitted artifacts matching the selected captured workspace targets;
- no scope, metadata or parser coverage gap.

Completion then re-verifies source bytes, profile and input identity, scope plan,
process receipt, stdout/stderr/result artifacts and resource release. A passed row has
`source_checkout_verified=true`, exit code zero and proven family departure.

## 7. Reuse

Reuse is opt-in (`reproducible=true`) and disabled whenever an effective input is
opaque, mutable, unversioned or incompletely discovered. Cargo reuse additionally
requires a versioned build-environment identity covering external tools not discovered
individually, such as linkers, wrappers, rustdoc and code generators.

A reusable source must be one original passed CheckRun:

- no `cached_from_check_id`;
- exit code zero and released resource;
- exact input fingerprint, scope plan and parser coverage;
- verified source/result/output bytes and process receipt;
- same project and same source candidate still accepted.

After off-DB verification, the final reservation transaction reruns this identity check.
A cached CheckRun is a derivative reference to that original evidence; it has no process
or exit code of its own. Cache chains are forbidden.

Acceptance follows the derivative to the original row, revalidates the original
Operation/process/coverage/artifacts and confirms the frozen accepted-source provenance.
A cached row with `exit_code=null` is therefore not a pass shortcut.

## 8. Acceptance handoff

The common acceptance gate requires the exact frozen requirement evidence plan,
candidate, dependency receipts, review coverage and required CheckProfile identities.

For every supplied CheckRun it rechecks:

- same Attempt and candidate;
- exact profile ID/revision and parser;
- passed state and complete coverage;
- settled applied original Operation;
- source checkout verification;
- original process/resource evidence;
- result and output artifact identity and bytes;
- original cache lineage where reuse was used.

Acceptance performs this gate before and after off-DB byte verification. A changed
manifest or current evidence prevents commit.

Check evidence proves the configured machine criterion. It does not replace independent
semantic review where the requirement demands review, and it does not repair
[AR-064](agent-operations/audit-register.md#ar-064).

## 9. Executor migration

Current source has two executor selections:

- `checks.executor = None`: legacy worker inside `swarm-kernel-host`;
- verified executor pin: standalone `swarm-checks`, with no fallback.

Both use the same `swarm_checks::execute_with_plan` custody and the same completion
validators, so the legacy route is not an evidence bypass. It is still duplicate
production execution.

Target cut:

1. configure standalone `swarm-checks` for all new CheckRuns;
2. inventory and disposition concrete active legacy runs;
3. keep historical evidence readable;
4. delete the in-host executor, fallback configuration, fixtures and dependency edges;
5. retain one Store control/recovery path.

Do not add an executor profile ladder or a compatibility service.

## 10. Configure and invoke

Start from the repository examples:

- [`config/checks.example.toml`](../config/checks.example.toml)
- [`config/task-checked.example.json`](../config/task-checked.example.json)
- [`config/source-capture.example.json`](../config/source-capture.example.json)
- [`config/check-run.example.json`](../config/check-run.example.json)

The examples are repository-specific. Keep checks disabled until the trusted profile,
targets, resource and executor pin are correct.

```powershell
swarm --request-id capture-1 source capture --file capture.json
# Read operation.get and retain result.candidate_ref.
swarm --request-id submit-1 task submit --file submission.json
swarm --request-id check-1 check run --file check.json
swarm check get CHECK_ID
```

Submission and CheckRun may occur in either order when both reference the same immutable
candidate. Read the original Operation and artifacts for the final machine report and
output references. Passing a check never accepts the Task, publishes code or closes an
Issue.

## 11. Construction and qualification

During implementation run formatting and minimal affected warnings-denied Clippy/build.
A vertical slice closes only with connected cases for:

- exact request replay versus conflict;
- fixed-source late completion and restart reconciliation;
- no command before Store Go;
- cancellation before spawn and during execution;
- host restart with live, empty, terminal and damaged process families;
- parser target/source identity and coverage gaps;
- cache hit, invalidation, no-chain and original-byte revalidation;
- acceptance recheck around off-DB bytes;
- standalone-only new work with no hidden fallback.

Installed Windows/Linux process behavior, power loss, native toolchains, escaped
descendants and load remain separate qualification.

## External contracts

These sources define platform/tool behavior, not product qualification:

- [Cargo JSON messages](https://doc.rust-lang.org/cargo/reference/external-tools.html)
- [Git batch objects](https://git-scm.com/docs/git-cat-file)
- [Windows Job Objects](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects)
- [Job membership](https://learn.microsoft.com/windows/win32/api/jobapi/nf-jobapi-isprocessinjob)
- [Job process list](https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_process_id_list)
- [Windows kernel-object namespaces](https://learn.microsoft.com/en-us/windows/win32/termserv/kernel-object-namespaces)
- [Linux `pidfd_open`](https://man7.org/linux/man-pages/man2/pidfd_open.2.html)
- [Linux `pidfd_send_signal`](https://man7.org/linux/man-pages/man2/pidfd_send_signal.2.html)
