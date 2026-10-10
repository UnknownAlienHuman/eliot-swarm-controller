# R35. Finite process completion: bounded exit, family departure and truthful capture

**PR task; production code has not been changed.**
Evidence baseline: `40591a295af94b1541ec2ba30afe8e3247701a71`, reviewed 8 October 2026.

This task owns finite, controller-started jobs:

- CheckRun execution in `swarm-checks`;
- standalone ScriptRun execution in `swarm-script-worker`;
- the check-input probe helper;
- the Store producer/consumer states required to reconcile incomplete cleanup.

It does not own long-lived module sessions, ACP sessions or externally owned services. Those remain R01/#27, R02/#28 and R18/#44. Git/Forge command execution may reuse the finished primitive later, but is not a reason to expand this task into a generic process framework before the named callers work.

## 1. Confirmed current failure chain

### 1.1. A configured execution timeout does not bound cleanup

`swarm-checks::execute` computes a command deadline and requests termination after cancellation/timeout. It then calls `wait_until_empty` with the same already-expired deadline.

Inside `wait_until_empty`, expiry only keeps setting `timed_out=true` and repeating `cancel_children()`. There is no cleanup deadline or maximum observation interval. A surviving descendant or repeated process-observation error blocks the CheckRun forever.

The abort/setup-error path has the same shape: `abort_owned_child` loops until the group reports empty and can call direct `child.wait()` without a bound.

### 1.2. Capture completion can block after the process family is empty

`finish_captures_after_group_empty` polls `JoinHandle::is_finished()` until both reader threads finish. It has diagnostics but no deadline.

Process-family emptiness does not prove pipe EOF. A handle inherited outside the tracked group, a platform pipe anomaly, or a blocked file write can leave a capture thread alive indefinitely. The function then never joins or returns.

### 1.3. Capture errors currently fabricate zero output

`finish_capture` maps any reader I/O error or panic to:

```text
bytes_written = 0
truncated = false
capture_complete = false
```

The file may already contain retained bytes. Reporting zero is not a conservative unknown; it is false evidence. The same disk/capture failure can also influence the child by closing a pipe, so child failure and capture failure must remain separate facts.

### 1.4. ScriptRun repeats the unbounded pattern twice

Both `swarm-script-worker` and the legacy kernel-host `scripts/runner.rs` implement the same sequence:

```text
poll child until execution deadline
request group cancellation
blocking child.wait()
while !group.children_empty(): cancel + sleep
join stdin/stdout/stderr threads
```

Every stage after the initial polling deadline is unbounded. The error path waits for group empty and then publishes empty stdout/stderr and `exit_code:null`, discarding output already captured before the later failure.

Maintaining two implementations has already created a drift surface. R35 selects the standalone worker as the production implementation and removes the legacy execution copy after its caller is migrated. Historical receipts remain readable; the old executor does not remain startable merely for compatibility.

### 1.5. Check-input cleanup can block inside `Drop`

`wait_owned_child` loops forever around `wait/try_wait`. `ProbeOwner::release` loops until group empty. `ProbeOwner::drop` contains another unbounded cancellation loop.

A destructor that can block forever can wedge the Store/check-input caller during ordinary error unwinding. `Drop` may request best-effort kill/owner close, but it cannot perform the authoritative wait loop.

### 1.6. Store settlement currently has no honest cleanup-pending state

The finite worker wants to return only after resource release. A Store check path currently rejects completion when `resource_released=false`, so the worker either waits forever or cannot publish a useful intermediate fact.

The missing state is not success or failure of the check/script itself. It is:

```text
native execution reached a known/unknown outcome
but exact process-family/capture cleanup is not yet proven
```

That state must retain the owner identity and be reconciled without replaying the command.

## 2. Three separate proofs

Do not use one boolean `finished`.

A finite child job has three independent axes:

```rust
enum DirectExit {
    Observed { exit_code: Option<i32> },
    ObservationUnknown,
}

enum FamilyDeparture {
    Confirmed,
    CleanupPending { owner_identity: ProcessOwnerIdentity },
    ObservationUnknown { owner_identity: ProcessOwnerIdentity },
}

enum CaptureDisposition {
    Complete(StreamEvidence),
    Incomplete {
        retained: PartialStreamEvidence,
        code: CaptureFailureCode,
    },
}
```

Names are proposed, not existing API. The persisted/wire form must remain bounded and schema-versioned.

Rules:

- direct child exit does not imply descendants departed;
- descendants departed does not imply capture EOF;
- capture complete does not prove exit code or success;
- a timeout is termination intent, not departure proof;
- output limit is a fact about retained evidence, not permission to forget real byte count;
- success requires the exact domain's existing semantic conditions **and** the required cleanup proofs.

## 3. Two deadlines after admission

Each job has distinct budgets:

1. **execution deadline** — existing plan/user policy;
2. **termination grace deadline** — starts at the first timeout/cancel/output-limit termination request;
3. **capture drain deadline** — starts after family departure is confirmed or after the killing owner is closed.

These are infrastructure bounds, not limits on agent reasoning or arbitrary “two attempts.” They must be configured in the trusted CheckRun/ScriptRun profile, not accepted from an untrusted invocation.

At the termination grace deadline:

- close/terminate the exact killing owner according to the existing Job/process-group contract;
- retain exact owner identity and the fact that cleanup was requested;
- stop blocking the worker;
- return `cleanup_pending` / `outcome_unknown`, never `resource_released=true`;
- do not disarm a still-live or unverified killing owner.

A later reconciler performs readback of exact group/Job departure. It does not spawn a replacement and does not repeat the original command.

## 4. One small finite-process completion component

Implement a private component in `swarm-process` only if it is immediately used by both `swarm-checks` and `swarm-script-worker`. Do not create an unused generic process runtime.

The component owns:

- an exact `Group`/Job/process-group owner;
- direct child handle while available;
- execution/termination/capture deadlines;
- bounded termination request count and observation diagnostics;
- capture lifecycle;
- conversion to a typed finite outcome.

Suggested operations:

```text
observe_child_exit
request_termination(reason)
observe_family_departure
poll_capture
close_killing_owner
finish_or_return_cleanup_pending
```

The caller retains domain-specific parsing and success rules. `swarm-process` does not know CheckRun, ScriptResult, Task or acceptance.

### 4.1. Capture must be interruptible

Do not solve this by dropping a blocked `JoinHandle`; that leaks a thread. Do not increase a sleep/queue bound.

Replace the uninterruptible reader-thread contract with a bounded capture implementation whose reads can be cancelled or polled:

- Unix: nonblocking pipe + poll/read under deadline;
- Windows: pollable/overlapped or `PeekNamedPipe`-based bounded reads;
- alternatively a dedicated capture helper only if its own process family and protocol are simpler and fully bounded.

Use the existing platform layer; do not add a PTY for ordinary pipe capture. PTY changes buffering and terminal semantics and is not a cleanup primitive.

Capture continues draining after retention cap so the child cannot block, but retained bytes, total observed bytes, truncation, I/O error and completeness remain separate fields.

### 4.2. Primary and cleanup errors stay separate

Persist:

```text
primary execution outcome/error
termination request status
family departure status
capture status for stdout/stderr
cleanup diagnostics
```

A failure to write diagnostics must not replace the primary child/capture error. A cleanup failure must not turn a failed command into success. A capture failure must not fabricate child nonzero exit.

## 5. CheckRun vertical slice

### 5.1. Worker

Migrate `swarm-checks::execute`:

- execution timeout/cancel starts termination grace;
- no unbounded `child.wait`, `wait_until_empty`, `abort_owned_child` or capture join;
- exact owner close/readback is recorded;
- partial capture evidence uses real retained file bytes/count/digest where safely readable;
- return a typed execution with `resource_released=false` only as an explicit cleanup-pending result, never a terminal passed/failed result.

### 5.2. Store/standalone host

Update the CheckRun consumer so cleanup-pending is a retained nonterminal state:

- resource slot remains held;
- original check command is never admitted again;
- exact owner identity is available to the reaper/readback path;
- when exact departure and capture disposition are resolved, settle the same CheckRun Operation once;
- if departure remains unknown, expose attention/diagnostic, not success;
- a missing `worker.lock` or completion file cannot prevent recording the cleanup uncertainty.

R09/review acceptance still requires the exact terminal passing CheckRun; cleanup-pending never satisfies it.

## 6. ScriptRun vertical slice and deletion of the duplicate executor

Use `swarm-script-worker` as the standalone execution owner.

- migrate it to the finite completion component;
- preserve captured bytes on post-start errors;
- publish `incomplete/cleanup_pending` with exact owner and capture status when cleanup cannot be proven;
- reconcile the same ScriptRun; no rerun of controller effects or script input;
- controller effects are applied only from a complete, identity-bound ScriptResult;
- output/capture uncertainty never produces an effect list.

Then remove the executable child/capture loop from `swarm-kernel-host/src/scripts/runner.rs` after all live callers use the standalone worker. Keep only historical readback/decoding required for retained receipts. Do not fix and maintain both copies indefinitely.

## 7. Check-input probe

- replace infinite `wait_owned_child` with execution + cleanup deadlines;
- `ProbeOwner::Drop` performs only nonblocking/best-effort owner close; authoritative wait happens in an explicit method;
- a failed probe retains bounded stderr/primary code and cleanup status;
- probe cleanup pending prevents use of an unverified toolchain result but does not block the Store thread forever;
- do not reinterpret an absent response as a clean empty output.

## 8. Donor findings

### 8.1. Ractor — stop-and-wait is explicit

A production user reported a real bug after assuming parent stop would run child cleanup. The maintainer explains that graceful stop without waiting creates an unstable state; kill is a last resort.

Take:

```text
request stop → wait exact children → parent complete
or deadline → kill → readback
```

Do not import Ractor.

Source: <https://github.com/slawlor/ractor/issues/398>

### 8.2. ACP Rust SDK — wrapper PID is not the process family

Issue #249 reproduced `npx/uvx` wrapper death while the real agent survived. Take the process-group/Job requirement. Do not adopt ACP's child guard as ELIOT authority.

Source: <https://github.com/agentclientprotocol/rust-sdk/issues/249>

### 8.3. portable-pty — I/O transport only

Portable PTY is useful for future interactive CLI adapters, but field reports on Windows include output, PATH, console and ConPTY behavior. A PTY handle is not process-family departure or capture completeness. R35 uses ordinary bounded pipes for finite checks/scripts.

### 8.4. Existing ELIOT process evidence

Reuse `swarm-process::Group`, exact Job/process-group identities and departure readers. Do not add a second process registry. The fix is to make waits bounded and outcomes truthful, not to replace the platform owner.

## 9. Exact tests

### 9.1. CheckRun

- direct child exits but descendant keeps stdout open;
- cancellation/timeout starts grace deadline and returns bounded cleanup-pending;
- exact group later becomes empty and the same CheckRun settles once;
- no second command execution;
- cleanup-pending does not satisfy acceptance or release resource early.

### 9.2. Capture

- child writes bytes, then capture file write fails: retained byte count is not reset to zero and `capture_complete=false`;
- output exceeds retention cap: total observed > retained, `truncated=true`, process does not block;
- family empty but pipe EOF absent: capture deadline produces incomplete evidence without leaked reader thread;
- capture error and child exit are both visible; neither masks the other.

### 9.3. Process observation

- `try_wait` error followed by exact family departure;
- kill request fails but Job close/readback proves departure;
- departure observation unavailable: owner identity retained and result remains unknown;
- PID reuse cannot satisfy readback.

### 9.4. ScriptRun

- timeout after partial stdout preserves output and does not apply effects;
- complete valid ScriptResult applies effects once;
- cleanup pending survives worker restart and resolves without replay;
- legacy kernel-host runner is not a startable production path after migration.

### 9.5. Probe

- helper ignores termination and spawns descendant;
- explicit release returns by deadline with cleanup uncertainty;
- dropping `ProbeOwner` never blocks the test thread;
- no unowned descendant remains after later readback/reaper.

Use paused time where possible and real Windows Job/Unix process-group integration fixtures for family departure. A helper-only state-machine test is insufficient.

## 10. Removal and boundaries

After migration remove:

- infinite `wait_until_empty`/`abort_owned_child` loops for finite checks;
- blocking post-timeout `child.wait()` in script execution;
- blocking capture-thread joins without deadlines;
- `ProbeOwner::Drop` wait loop;
- false zero-output fallback;
- duplicate executable loop in legacy `scripts/runner.rs`;
- comments claiming a timeout while cleanup can still wait forever.

Do not add:

- a PTY to noninteractive checks/scripts;
- an external process supervisor service;
- retries of the original command;
- a global timeout applied to long-lived agent sessions;
- success when family/capture evidence is unknown.

## 11. Ownership and integration

- R01/#27 owns long-lived module process identity and generic supervisor lifecycle.
- R02/#28 owns OpenCode NativeOwner shutdown over R01.
- R18/#44 owns Command ACP sessions/process family over R01.
- R09/#35 and R22/#48 consume terminal CheckRun evidence but do not define process cleanup.
- R34/#60 owns automation poison-fact isolation, not child execution.
- R35 owns finite CheckRun/ScriptRun/probe completion and the Store states needed for exact cleanup readback.

Implementation order:

1. typed finite outcome and deadlines in `swarm-process` with CheckRun caller;
2. CheckRun Store cleanup-pending/readback;
3. bounded capture implementation;
4. standalone ScriptRun migration;
5. ScriptRun cleanup readback;
6. check-input probe;
7. remove legacy script execution copy;
8. scoped Clippy and exact platform tests.

The shared component is not accepted as a standalone abstraction without the CheckRun caller in the same commit series.

## 12. Minimal gate after connected code

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-process -p swarm-checks -p swarm-script-worker -p swarm-kernel-host --lib --bins -- -D warnings
```

Then the exact program/platform tests in §9. Broad workspace/native/model qualification remains the final phase.
