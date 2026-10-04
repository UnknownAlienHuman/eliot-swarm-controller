# Forge publication first slice

Issue #10 adds one narrow operation: publish the exact Git commit retained by
the currently accepted source-snapshot candidate to one configured branch ref.
It does not create or merge pull requests, choose a repository from request
data, or change Task acceptance.

## Local configuration

Publication is disabled by default. The operator enables it in the controller
configuration and maps each Task `project_id` to one local checkout, one
canonical repository identity, one remote alias, one accepted policy revision
and an allowlist of exact branch refs. Relative paths resolve against the
configuration file's directory; enabled paths must resolve to existing local
Git and repository paths. The request cannot override those values.

```toml
[forge]
enabled = true
git_executable = "C:/Program Files/Git/cmd/git.exe"
timeout_seconds = 120
max_output_bytes = 32768

[forge.projects."project-id"]
canonical_repository = "github.com/owner/repository"
repository_path = "C:/repositories/repository"
remote_name = "origin"
policy_revision = "owner-policy-v1"
target_refs = ["refs/heads/main"]
```

The configured push URL must resolve to exactly one credential-free
`host/owner/repository` identity matching `canonical_repository`. HTTPS,
`ssh://git@...` and `git@host:...` URLs are supported. Credentials stay in
the operator's existing native Git credential setup; neither URLs nor raw Git
stderr are stored in Operations, observations, or returned diagnostics.

## Request and identity checks

`forge.publish_ref` requires a caller-owned `client_request_id`, `attempt_id`,
Task revision, `submission_ref`, applied `accepted_operation_id`, exact
`candidate_ref`, expected policy revision, allowlisted target ref, and exactly
one of `expected_old_ref` or `expected_create=true`. A force field is rejected.
The caller must be the local operator or current GM. At admission and again
immediately before the write, the controller verifies that the Task is still
accepted at that revision, the accepted operation and submission name the
same Attempt and candidate, the Attempt snapshot names the configured policy,
and the candidate is a complete `source_snapshot` with the retained commit and
tree identity. The immutable intent also records the GM epoch at admission
(zero before any GM designation). Queued work is rechecked against that epoch
before it can start and again after read-only remote preflight, immediately
before the push. An epoch change settles the never-sent Operation as
`stale_gm_epoch`; only the local operator may cancel that stale Operation in
v1. `sending` and `outcome_unknown` work remains readback-only across handover.

The durable Operation records the canonical repository, candidate artifact
digest, full commit and tree IDs, configured remote alias and target ref,
expected old value or create intent, `force=false`, and policy revision before
any network write. The request contains no local path, Git executable, remote
URL, credential, or arbitrary command.

An explicitly selected `publication` automation consumes applied
`task.acceptance` observations. Its entry owner must be the current GM, and
its settings select the exact allowlisted target and expected-old/create
intent. The typed context records the real manager, technical requester,
entry revision and immutable acceptance cause; it does not create a manager
credential. Admission, worker start and the write boundary recheck current
authority, acceptance, candidate and local Forge policy. See the
[configuration contract](agent-operations/configuration.md) for the optional
settings and explicit `include_existing` activation choice.

Manual and automatic requests share the exact repository/candidate/commit/tree/
target/expected-old-or-create slot. A duplicate links to the retained winner
without another push. Sending, unknown and applied outcomes continue to own
that slot. Proven no-effect cancellation, stale epoch, pre-write failure or
completed Git rejection releases it for a new request under current rights;
the old operation remains in history and is neither adopted nor replayed.

## Effect and recovery

`forge.publish_ref` ends at durable admission: a new intent returns a queued
Operation receipt, while an exact duplicate returns a settled coalesced
receipt naming its retained winner. Admission notifies the Store change signal. It
does not run Git in the request/IPC future. The host-owned supervisor polls;
each pass reads earlier uncertain work first, then starts queued work only
while `host.mode.new_work` is enabled. Host shutdown awaits the supervisor
task; the Git work is not detached from the lifecycle owner.

The worker verifies the retained manifest and that the configured checkout
contains the exact accepted commit and tree. It checks the configured remote,
rejects mirror mode, push options, custom SSH/server commands and remote
helpers, then reads the exact target ref. Only an exact match to the expected
old object (or absence for an explicit create) permits one ordinary,
non-force Git push of the full commit to the one full ref. Hooks that could
alter the push are skipped; tags and submodule pushes are disabled. Output is
bounded, prompts are disabled, and a finite timeout is applied.

On Windows, each native Git process is assigned to a dedicated [Job Object](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects)
at process creation with the documented [`PROC_THREAD_ATTRIBUTE_JOB_LIST`](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-updateprocthreadattribute)
attribute and `KILL_ON_JOB_CLOSE`. Descendants inherit that job. The child receives only its
stdin/stdout/stderr handles; the environment block is copied from the current
Unicode environment, sanitized for Git overrides, and sorted with Windows
ordinal case-insensitive comparison. Output is polled from anonymous pipes,
so no reader thread can remain blocked on a descendant's inherited pipe. On a
timeout the complete job is terminated; after Git exits, any remaining job
members are also terminated and the active-process count is polled to zero
under a finite cleanup deadline before a successful return. A post-spawn error
gets one additional bounded terminate-and-query attempt. If that later query
proves the job empty after an earlier termination deadline, the runner reports
`FORGE_GIT_TREE_CLEANUP_DELAYED`; the Store may then use its ordinary exact-ref
readback rules. If neither query proves the job empty, the runner returns
`FORGE_GIT_TREE_TERMINATION`, which makes the Store hold further publication
for that repository. This error means only that the runner could not prove its
local Job Object empty; it does not mean an external write was attempted. The
durable `publication_may_have_started` marker is recorded separately at the
write boundary, so validation or remote-preflight failures retain `false`. The
hold remains sticky and requires manual operator intervention; ref readback
cannot settle that operation as applied or clear the hold.
Drop performs one final best-effort termination and polls
for at most two seconds, then closes the final Job handle so
`KILL_ON_JOB_CLOSE` applies. Cleanup waiting is bounded by at most two
ten-second active-process polls plus that two-second Drop poll, in addition to
the configured command timeout. If Windows cannot atomically assign the job
or establish the bounded pipes, process creation fails closed. This path
requires the Windows job-list process creation attribute (Windows 10 / Windows
Server 2016 or newer).

After the push runner proves its process tree empty, the controller reads the
exact ref. A matching candidate commit is recorded as applied even if Git's
response was lost. A completed Git rejection with the expected ref still
present is recorded as failed. Timeout, unreadable readback, or any other ref
value remains `outcome_unknown`. Each supervisor pass performs only exact
remote readback for `sending` and `outcome_unknown`; it never repeats those
pushes. A `process_tree_unconfirmed` hold may record later readback, but a
matching candidate does not settle the operation or clear the sticky hold.
Ordinary `outcome_unknown` work with confirmed process-tree cleanup can settle
from an exact matching readback. A durable `queued`
Operation has not crossed the write boundary. If shutdown occurs before the
supervisor moves it to `sending`, a later pass may start it once after
rechecking current operator/GM authority, accepted candidate, policy mapping,
execution mode, and local source identity. If it has reached `sending`,
recovery is readback-only even after GM handover. A later readback proves
success only when it sees the exact candidate commit. A different or missing
ref does not prove that an interrupted push can never finish, so the Operation
remains unknown for operator resolution.

Remote readback cannot provide an atomic compare-and-swap with the Git push
protocol used here. A third party can move the ref after the preflight read and
before the ordinary push; Git's non-force fast-forward rule still prevents
rewinding a ref, but it does not bind the push to the exact expected-old value.
The implementation therefore fails closed on any value observed before the
write and documents the remaining race instead of claiming atomic CAS.

Publication and acceptance are separate facts. A failed or unresolved
publication does not revoke acceptance, and this operation does not perform
bookkeeping, cleanup, pull-request creation, or merging.
