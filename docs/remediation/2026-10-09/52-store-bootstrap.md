# R52. SQLite writer bootstrap: caller timeout from the first query, verification before commit

**Status:** production slice implemented and qualified by rustfmt plus changed-line Clippy on Ubuntu and Windows. Tests remain in the final product phase.

**Source baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71`. Primary path: `crates/swarm-store/src/lib.rs::open_writer_inner`.

## 1. Result

Opening a writer now has one truthful success boundary:

```text
validate identity, SQLite version and timeout range without touching the file
→ open connection
→ install caller busy_timeout before the first SQLite query
→ inspect application/user/schema identity
→ only for an accepted ELIOT or allowed empty database:
     apply foreign_keys=ON, journal_mode=WAL, synchronous=FULL
     verify timeout + required pragmas
→ BEGIN IMMEDIATE
→ initialize/verify schema and run the kernel callback
→ verify required connection postconditions through the Transaction
→ COMMIT
→ return with no fallible post-commit work
```

The implementation adds no Store, pool, migration framework, retry loop or dependency.

## 2. Confirmed defects on `main`

The original order was:

```text
open
→ user_version / application_id / sqlite_schema queries
→ foreign_keys / WAL / FULL
→ caller busy_timeout
→ BEGIN IMMEDIATE
→ initialize
→ COMMIT
→ verify pragmas
```

Two defects followed.

### 2.1. `WriterOptions.busy_timeout` did not govern the whole open path

The first identity queries and WAL transition used rusqlite's connection default rather than the caller's value. A zero or custom timeout therefore described only the later part of startup.

The branch now validates the `Duration` before opening and applies `Connection::busy_timeout` immediately after open, before any query.

### 2.2. Startup could return failure after durable bootstrap

`verify_writer_pragmas(&db)` ran after `tx.commit()`. A verification/query failure therefore produced a startup error after schema tags, digest and initializer writes had become durable.

The branch now verifies through the live `Transaction` before commit. After a successful commit, `open_writer_inner` returns directly.

## 3. Audit correction discovered during implementation

The first handoff proposed applying all pragmas before identity inspection. That is unsafe.

`PRAGMA journal_mode=WAL` is a persistent database effect. Applying it before checking `application_id`, `user_version` and schema emptiness could mutate a foreign SQLite file and only then return `SchemaMismatch`.

Therefore the exact order is deliberately split:

```text
before identity:
  busy_timeout only

after identity accepts this database:
  foreign_keys
  journal_mode=WAL
  synchronous=FULL
  full postcondition verification
```

The public timeout controls every SQLite operation, while durable/database-specific configuration is never applied to a rejected foreign database.

## 4. Implemented private types and helpers

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WriterPragmaExpectation {
    busy_timeout_ms: i32,
}

fn checked_timeout_ms(timeout: Duration) -> Result<i32, Error>;
fn configure_writer_connection(db: &Connection) -> Result<(), Error>;
fn verify_writer_connection(
    db: &Connection,
    expected: WriterPragmaExpectation,
) -> Result<(), Error>;
```

`checked_timeout_ms` mirrors the millisecond range accepted by rusqlite's `busy_timeout` before calling the panic-prone conversion. `Error::InvalidBusyTimeout` maps to the existing stable `STORE_CONFIGURATION` code.

`verify_writer_connection` checks:

```text
PRAGMA busy_timeout == requested milliseconds
PRAGMA foreign_keys == 1
PRAGMA journal_mode equals wal case-insensitively
PRAGMA synchronous == 2
```

There is no public options framework or test callback in the product API.

## 5. Transaction boundary

Current branch flow:

```rust
identity.validate()?;
validate SQLite minimum;
let expected = checked timeout expectation;

let mut db = open(...)?;
db.busy_timeout(options.busy_timeout)?;
inspect exact database identity;

configure_writer_connection(&db)?;
verify_writer_connection(&db, expected)?;

let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
initialize or verify schema/digest;
let initialized = initialize(&tx, is_new)?;
verify_writer_connection(&tx, expected)?;
tx.commit()?;
Ok((db, initialized))
```

Any initializer or precommit verification error drops the transaction and uses rusqlite rollback-on-drop. No manual `BEGIN`, `COMMIT` or `ROLLBACK` SQL was added.

The WAL transition remains outside the transaction because it is database configuration and may require its own lock transition. It occurs only after identity acceptance.

## 6. Reader boundary

`open_reader` remains a separate read-only contract:

```text
validate identity and timeout range
→ open read-only
→ apply busy timeout
→ query_only=ON
→ exact application/user version
→ exact schema digest
```

The branch reuses only `checked_timeout_ms`; it does not force writer pragmas onto the reader.

## 7. Failure semantics

| Stage | Returned result | Durable effect from this call |
|---|---|---|
| identity/options/version validation | typed configuration/version error | none; file not opened/created |
| open | SQLite error | no ELIOT schema initialization |
| caller timeout installation / identity queries | SQLite or schema error | no WAL/config mutation by ELIOT |
| accepted database pragma setup | SQLite/configuration error | WAL may already be selected for the accepted database; no schema callback writes |
| `BEGIN IMMEDIATE` | SQLite busy/error | no schema callback writes |
| schema/digest/initializer | Store or initializer error | transaction rolled back |
| precommit postcondition verification | Store configuration error | transaction rolled back |
| commit | SQLite commit result | no later validation can turn a committed success path into a startup error |
| after commit | direct return | success |

Do not add an outer retry loop for `SQLITE_BUSY`. The configured SQLite busy handler remains the one waiting policy.

## 8. Remaining qualification scenarios

Tests are intentionally deferred to the final product phase, but the implementation must eventually prove:

1. **Caller timeout from the first query.** An exact existing database under another connection's exclusive lock returns promptly with `Duration::ZERO`, without waiting rusqlite's default timeout.
2. **Foreign database preservation.** A mismatched application/user identity is rejected without changing its journal mode.
3. **Initializer rollback.** A sentinel written by an initializer that returns an error is absent after the call.
4. **Precommit verification rollback.** An injected private verifier failure after sentinel creation leaves no committed sentinel/schema tags/digest.
5. **Create/reopen.** `is_new` is true exactly once; reopen verifies digest and reports the configured pragmas.
6. **Oversized timeout.** Returns `InvalidBusyTimeout`, creates no file and does not panic.
7. **Reader.** Remains read-only/query-only with its exact identity checks.

A future private fault-injection helper may be added only inside tests. It must not become public runtime API.

## 9. Removed old behavior

- caller timeout applied after identity/WAL work;
- post-commit `verify_writer_pragmas(&db)`;
- verifier that ignored the requested timeout;
- panic-prone unvalidated timeout path in writer/reader;
- the unsafe handoff instruction to set WAL before database identity.

No compatibility branch preserves the old ordering.

## 10. Cross-PR ownership

- #76 landed the independent rustfmt/Clippy attribution gate used for this qualification; it is no longer an implementation dependency.
- #63 owns DataRoot/state markers, not SQLite connection configuration.
- #71 owns optional supervisors, not Store bootstrap.
- #60 owns automation-domain savepoints after a successful Store open.

R52 owns only `swarm-store` opening/configuration and its eventual narrow integration tests.

## 11. Gate and current evidence

Exact head `f9cf53687335e3ea5962a44680738e8227ddafd0`, workflow run `37881420236`:

- changed-path/package classification passed;
- documentation and changed-file rustfmt validation passed;
- the selected package plus its real local reverse-dependency closure compiled and completed Clippy on Ubuntu;
- the same scoped compiler/Clippy gate passed on Windows;
- no compiler error or warning owned by a changed Rust line remained;
- unrelated historical warnings, if any, were reported separately rather than attributed to this slice.

Changed integration tests were correctly skipped because this slice changes no integration test target. Broad tests, locked-database behavior and cross-platform lock qualification remain the final product phase.
