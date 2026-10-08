# R45 companion. Forge publication: exact expected-ref CAS, not a blind force push

**Status:** implementation handoff. Production Forge worker, publication Operation and remote refs are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Audit correction

The broad allegation “Forge pushes without `--force-with-lease`, therefore two publications silently overwrite each other” is not accurate on the current source.

Current worker behavior is:

```text
read exact target ref
→ compare it with expected_old_ref / expected_create
→ ordinary non-force git push
→ read exact target ref again
→ require candidate_ref
```

An ordinary branch push rejects a non-fast-forward update. Therefore the current path does **not** silently rewind an unrelated divergent remote branch merely because `--force-with-lease` is absent.

The remaining defect is narrower and real: the pre-read and push are not one compare-and-swap operation. If another writer advances the branch after the pre-read to a commit already contained in the candidate's ancestry, the ordinary push remains a valid fast-forward and succeeds even though the retained `expected_old_ref` precondition is no longer true.

That does not lose commits, but it violates the exact publication contract. `expected_old_ref` is currently a diagnostic precheck, not the final effect fence.

## 2. Required invariant

For an existing branch:

```text
remote target must still equal expected_old_ref at push admission
```

For creation:

```text
remote target must still not exist at push admission
```

The immutable Store request already carries:

```text
target_ref
expected_old_ref
expected_create
candidate_ref
```

Do not introduce another publication token, lock service or Git state table.

## 3. Minimal implementation

Keep the existing exact pre-read for bounded diagnostics and early refusal. Change only the effect command so Git enforces the same retained precondition atomically with ref update.

For an existing branch, construct the exact lease argument from the retained request:

```text
--force-with-lease=<target_ref>:<expected_old_ref>
```

For creation, use the explicit empty expected value supported by Git's lease syntax:

```text
--force-with-lease=<target_ref>:
```

The word `force` in this option must not be treated as permission to publish an arbitrary rewrite. In this path:

- `PublicationIntent.force` remains `false`;
- the source ref remains the exact accepted candidate;
- the destination remains the exact validated `refs/heads/*` target;
- the expected value comes only from the immutable Store request;
- a lease mismatch is a no-write refusal;
- no retry substitutes a newer observed remote value.

Prefer an argv builder such as:

```rust
fn exact_ref_lease(target_ref: &str, expected_old_ref: Option<&str>, expected_create: bool) -> Result<String>;
```

It returns one validated argument and owns no process, policy or Store access. If the existing worker already has a more local argv builder, extend it instead of adding another abstraction.

## 4. Result classification

R45's lifecycle work and this companion meet at the publication result boundary.

### Lease mismatch before write

Retain a structured, method-specific terminal result such as:

```text
publication_not_applied
reason = remote_ref_changed
write_attempted = false
expected_old_ref
observed_before_ref if available
```

Do not flatten it into `coalesced` or `Applied`.

### Push response lost or process observation uncertain

Do not repeat the push. Use the existing exact post-effect remote readback:

- remote ref equals candidate → applied;
- remote ref equals retained expected old value / remains absent and no write evidence exists → exact no-write result only when current process evidence proves it;
- any other ref or unavailable readback → outcome unknown / attention;
- never replace `expected_old_ref` with the newly observed value and retry automatically.

### Final readback

Success still requires the exact target ref to equal `candidate_ref`. A successful git exit alone is insufficient.

## 5. Public-path fixtures

Use the existing Store publication admission and real worker argv/readback seam. Helper-only string tests are not enough.

1. `forge_exact_lease_publishes_when_remote_ref_is_unchanged`
   - retained expected ref equals remote;
   - one update to candidate;
   - final readback exact.

2. `forge_exact_lease_rejects_divergent_remote_without_overwrite`
   - remote moves to an unrelated commit after pre-read;
   - lease fails;
   - remote remains untouched;
   - terminal result says not applied.

3. `forge_exact_lease_rejects_intermediate_fast_forward_race`
   - pre-read sees A;
   - another writer advances A→B;
   - candidate C contains B;
   - current ordinary push would accept B→C;
   - exact lease for A must reject because the retained precondition changed.

4. `forge_exact_create_lease_loses_creation_race_without_overwrite`
   - request expects no branch;
   - another writer creates it before push;
   - current publication performs no update.

5. `forge_lost_push_response_uses_readback_without_replay`
   - update may have happened;
   - same Operation is reconciled through remote ref;
   - worker command is executed once.

6. `forge_lease_argument_uses_retained_request_not_second_preflight`
   - remote changes after pre-read;
   - effect argv still names original expected ref;
   - no mutation of immutable request/effective identity.

Record argv in the worker fixture without exposing credentials or repository secrets.

## 6. Simplification and deletion

After migration:

- delete comments or result fields claiming the first remote read is the concurrency fence;
- delete any branch that refreshes `expected_old_ref` immediately before retry;
- keep one exact pre-read and one exact final readback;
- keep ordinary publication authority and accepted-candidate validation;
- do not add a generic Git transaction framework, remote lock or second publication scheduler.

The expected production change should be small: one argv construction, structured error mapping, fixtures and removal of misleading concurrency claims.

## 7. Ownership and ordering

R45/#68 owns:

- truthful existing publication Operation state/result classification;
- this exact remote-ref effect fence;
- final publication disposition consumed by automation.

The Forge worker owns Git execution and bounded capture. Store owns immutable intent, authority and readback interpretation. Neither layer invents the other's identity.

Coordinate with any concurrent `swarm-forge-worker` process/capture work; do not create a second child runner. This companion does not depend on a donor runtime.

Recommended order:

```text
1. typed Operation lifecycle loader/classifier
2. exact publication request/result validator
3. exact lease argv in existing Forge worker
4. structured lease-mismatch outcome
5. final readback and lost-response fixtures
6. remove generic coalesced/pre-read-as-fence branches
```

## 8. Gate

After connected code:

```sh
cargo fmt --all -- --check
cargo clippy --locked \
  -p swarm-forge-worker \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

Then run the exact Forge fixtures above. Live GitHub publication remains a later qualification phase and must use a disposable test ref, never a production branch.
