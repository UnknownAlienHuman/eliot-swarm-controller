# Repository commit hook

ELIOT can install one optional, repository-local `post-commit` observer for a
configured project. The event is `git.post_commit`, phase `post_commit`, and
has no veto power because Git has already created the commit. The hook does
not run a model, start an auditor, or make a check inline.

## Scope and credential

An authorized manager or local Operator sets up a source for a project that
already has an active workspace registration. The Store snapshots the project,
canonical repository, registration ID and generation. Before contacting the
Store, the local setup command generates and saves a dedicated `HookSource`
credential and a stable pending source/request identity in private
repository-local files. The Store registers only the client-token hash and
returns public source metadata. Credentials are excluded from source readback
and ordinary operation results. A lost reply retains the private preparation;
retrying the exact identity and credential reads the existing source without
issuing another credential. A successor authorized GM can recover that same
source while the original issuer and revocation state remain unchanged.

HookSource authority is limited to `hook.emit` for its own source and bounded
`hook.source.get` readback. It cannot discover methods or call other
application methods. Setup, readback by a manager, and revocation use current
manager or local-Operator authority. Revocation disables the credential and
source in one Store transaction. Store-only revocation leaves the wrapper in
place but inert; the local install-revoke command also restores the previous
hook when readback proves it is safe.

## Local installation

Preview resolves the exact configured repository and Git executable, checks
the effective hook directory, current `HEAD`, and existing `post-commit` and
`post-commit.exe` files. An explicit `core.hooksPath` is unsupported and left
untouched. Setup does not change global Git configuration, PATH, or the
repository's configured hook path.

Apply re-runs preview and refuses changes since that preview. The wrapper is
installed only in the resolved repository Git directory. Existing
extensionless hook bytes and mode are preserved in a source-specific backup;
an existing active extensionless hook is chained. A Windows `post-commit.exe`
stays in place and is chained when Git would run it. Readback reports whether the installed wrapper and backup still
match their digests. Revocation restores the prior hook only when those bytes
still match; if a user edited the wrapper or backup, the CLI leaves the local
files in place and the already-revoked wrapper is inert.

The wrapper captures the full commit object ID before chaining the previous
hook, then starts the optional callback with detached standard streams. It
returns the prior hook's exit status and ignores callback failure, so the
optional observer cannot block an already-completed commit. The Store does
not trust a caller-supplied repository path: it derives the path from the
active registration and verifies the exact captured object as a commit using
the configured Git executable before recording it.

## Durable event and automation boundary

Verified facts are written to `controller:hooks` as `git.post_commit`, keyed
by `{source_id}:{full_commit_oid}`. The closed payload records schema version,
source and project IDs, canonical repository, registration identity, exact
commit ID, and `readback_verified: true`. Duplicate delivery of the same
source and commit resolves to the existing immutable observation. Bounded
readback exposes those facts without exposing the token.

An observation alone never creates review or check work. The existing bounded
automation intake and shared automation reconciler may route only a fact whose
source registration, project, repository and commit match retained Store
evidence and whose current manager-owned automation is enabled for a
compatible existing action. A commit with no matching applied submission is
not promoted into an audit request. The normal typed action admission,
operation receipt, and readback remain authoritative; uncertain effects use
their existing readback path rather than timer replay.

An entry opts in with `hook_commit = { source_id = "..." }` on its existing
ReviewDispatch action; `entry.enabled` remains its activation switch. The
verified commit and applied submission can arrive in either order. Ordinary
TaskSubmission entries continue using their existing path. A revoked, missing
or stale selected source produces a scoped pending/readback diagnosis and
cannot create a review action.

Repository registration changes make the source stale for new events. Facts
already recorded remain historical evidence and can still be read back. The
installer is optional and does not change ordinary commit behavior when the
ELIOT process or Store is unavailable.

## Bounded delivery retries and event-rule boundary

A direct `swarm hook emit` makes at most three `hook.emit` attempts for the
same `{source_id, commit_oid}`: the first call, then at most two retries after
100 ms and 400 ms. It retries only `HOST_UNAVAILABLE` and `OUTCOME_UNKNOWN`.
Every attempt carries that fixed identity; Store verifies and deduplicates the
source/commit pair against the immutable observation. Other errors and an
invalid acknowledgment stop immediately. A successful CLI acknowledgment
names the same source and commit, reports `readback_verified: true`, and
includes the retained observation ID.

When all three attempts fail, the CLI returns the last bounded error. There is
no durable retry queue or timer replay. A later invocation of the installed
post-commit wrapper may resubmit the same fact, which Store deduplicates if the
source and commit match. The installed wrapper runs the callback detached and
suppresses its output, so a callback error cannot change Git's already-completed
commit or appear as a hook failure. Use `hook.source.get` readback to resolve
whether a fact was retained.

`event_rules` controls downstream automatic ReviewDispatch. An absent or
`null` value preserves the compatibility TaskSubmission route; `[]` explicitly
selects no event route. With `[]`, an enabled entry does not automatically
assign reviews from TaskSubmission facts and does not join hook commits to
applied submissions for ReviewDispatch. This changes routing, not fact intake:
`hook.emit` still authenticates and retains verified commit facts, and Store
continues to retain TaskSubmission observations. Direct `review.assign` and
other explicitly selected actions remain available under their own authority.
The exact typed rule and configuration examples are in
[Module API contracts](agent-operations/module-api.md).
