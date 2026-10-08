# R38 companion — `hook.emit` must reach Store without a fabricated request ID

**Status:** implementation handoff. Production code is unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71`.

## 1. Confirmed failure

The dedicated CLI path is currently unreachable before IPC:

```text
Command::Hook::Emit
  → params = {source_id, commit_oid}
  → is_hook_emit = true
  → reject --request-id
  → skip ordinary mutation client_request_id injection
  → hook_emit_with_retry(params)
  → model::fields(params, [source_id, commit_oid, client_request_id])
  → INVALID_PARAMS on every invocation
```

`hook_emit_with_retry` then constructs `exact_params` containing only `source_id` and `commit_oid`, so its own required `client_request_id` would be discarded even if a caller could supply one.

This is not a missing retry feature. The current implementation contradicts its own identity model.

## 2. Preserve the correct identity

`hook.emit` is an immutable observed fact, not a generic effect mutation:

```text
identity = exact (source_id, commit_oid)
```

Store `hooks::emit` verifies the configured repository and exact commit, inserts one immutable observation under that identity, and returns the retained observation on an identical duplicate. This is the reason bounded same-identity retry is safe.

Do **not** add a random `client_request_id`, Operation row, alias method or compatibility branch.

## 3. Minimal code change

In `crates/swarm-kernel-host/src/main.rs`:

1. Keep `--request-id` rejected for `hook.emit`.
2. Change `hook_emit_with_retry` input validation to accept exactly:

   ```text
   source_id
   commit_oid
   ```

3. Keep constructing the same closed `exact_params` object from those two fields.
4. Keep retries restricted to `HOST_UNAVAILABLE | OUTCOME_UNKNOWN` and to the same immutable identity.
5. Keep `hook_emit_ack_matches` bound to source, commit, `readback_verified`, positive observation ID, and exactly one of `recorded`/`duplicate`.
6. Remove the unreachable generic `HOOK_EMIT_RETRY_EXHAUSTED` tail if exhaustive control flow still proves it dead after the change. Do not retain dead code for a theoretical fourth exit path.

No Store, schema, credential, hook wrapper or automation-intake change is required for this fix.

## 4. Public-path fixtures to add with implementation

Use the real CLI mapping and an instrumented IPC/Store boundary; a helper-only test is insufficient.

- `hook_emit_cli_reaches_store_without_request_id`
- `hook_emit_rejects_request_id_override_before_ipc`
- `hook_emit_lost_reply_retries_same_fact_once`
- `hook_emit_identical_retry_returns_duplicate_observation`
- `hook_emit_changed_commit_is_a_distinct_fact`
- `hook_emit_invalid_ack_is_not_reported_success`
- `hook_emit_nonretryable_store_error_is_not_replayed`

The tests must assert exact forwarded method/params and Store observation count, not only `is_ok()`/`is_err()`.

## 5. Relation to the main R38 slice

The main R38 document owns resumable setup/install/revoke and bounded Git readback. This companion owns only event-delivery reachability after an installed wrapper invokes the CLI.

Implementation order inside the same PR:

1. fix the two-field `hook.emit` CLI path;
2. land manifest-v2 install/revoke reconciliation;
3. consume R35 finite-process capture for local Git readback;
4. remove replaced one-shot/reader-thread paths.

A working install that invokes an unreachable emitter is not complete. Conversely, this two-field fix does not make local installation crash-safe by itself.

## 6. Gate after connected code

```sh
cargo fmt --all -- --check
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Broad/native qualification remains a later project phase. The branch currently changes documentation only.
