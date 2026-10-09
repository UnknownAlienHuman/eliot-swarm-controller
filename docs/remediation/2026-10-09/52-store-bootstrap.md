# R52. SQLite writer bootstrap: options before I/O, verification before commit

**Status:** implementation handoff. Production code is unchanged in this branch.

**Source baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71`. Primary path: `crates/swarm-store/src/lib.rs::open_writer_inner`.

## 1. Result

Opening a writer has one exact success boundary:

```text
open connection
→ install caller WriterOptions before any schema/pragma operation
→ inspect database identity
→ apply and verify writer pragmas
→ begin IMMEDIATE transaction
→ initialize schema/kernel state
→ re-verify required connection postconditions
→ commit
→ return success with no fallible post-commit validation
```

If any required postcondition fails, base schema, tags, schema digest and kernel bootstrap callback writes are not committed.

No new Store, migration framework, connection pool or retry loop is introduced.

## 2. Audit correction

The initial suspicion said the writer always reaches `PRAGMA journal_mode=WAL` with no busy timeout. That statement is too broad:

- rusqlite currently installs a 5000 ms busy timeout on a newly opened connection;
- all current kernel callers use `WriterOptions::default()`, also five seconds.

Therefore a current default startup does not necessarily fail immediately on the first lock.

Two real contract defects remain.

### 2.1. Custom `WriterOptions.busy_timeout` is applied late

Current order:

```rust
Connection::open(...)
pragma_query user_version
pragma_query application_id
SELECT sqlite_schema
PRAGMA foreign_keys=ON
PRAGMA journal_mode=WAL
PRAGMA synchronous=FULL
busy_timeout(options.busy_timeout)
```

The public option does not govern the identity queries or the potentially locking WAL transition. A caller asking for zero, short or long wait receives rusqlite's default behavior during part of the open path.

### 2.2. Bootstrap commits before its own postcondition check

Current order:

```rust
initialize(&tx)
tx.commit()
verify_writer_pragmas(&db)
```

If verification or its query fails, `open_writer` returns an error after schema/bootstrap writes are already durable. The caller observes startup failure, but the next process sees an initialized database.

That is not a data-overwrite bug; it is a false transaction boundary and ambiguous startup receipt.

## 3. Existing APIs to use

Use only current rusqlite/SQLite primitives:

- `Connection::busy_timeout`;
- `Connection::pragma_update` / `pragma_query_value`;
- `TransactionBehavior::Immediate`;
- rollback-on-drop `Transaction`;
- `Transaction::commit`.

No dependency addition is needed.

`Transaction` dereferences to the connection for read-only pragma verification. Do not issue manual SQL `BEGIN/COMMIT/ROLLBACK` strings.

## 4. One connection configuration helper

Extract one private helper:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WriterPragmaExpectation {
    busy_timeout_ms: i32,
}

fn configure_writer_connection(
    db: &Connection,
    options: WriterOptions,
) -> Result<WriterPragmaExpectation, Error>;

fn verify_writer_connection(
    db: &Connection,
    expected: WriterPragmaExpectation,
) -> Result<(), Error>;
```

### 4.1. Timeout conversion

Convert before calling rusqlite:

```rust
let milliseconds = i32::try_from(options.busy_timeout.as_millis())
    .map_err(|_| Error::InvalidBusyTimeout)?;
```

Zero is valid and means no waiting. Avoid `Connection::busy_timeout` panic on an unrepresentable `Duration`.

Add one precise error variant if needed:

```rust
InvalidBusyTimeout
```

It maps to existing stable code `STORE_CONFIGURATION`. Do not add a general options DSL.

### 4.2. Apply order

Immediately after `Connection::open*`:

```text
busy_timeout
foreign_keys=ON
journal_mode=WAL
synchronous=FULL
```

Then verify:

```text
PRAGMA busy_timeout == requested milliseconds
PRAGMA foreign_keys == 1
PRAGMA journal_mode equals wal case-insensitively
PRAGMA synchronous == 2
```

Only after this configuration/verification read `user_version`, `application_id` and `sqlite_schema`.

Applying busy timeout first ensures every later SQLite operation follows the caller's requested lock policy.

## 5. Correct transaction boundary

Refactor `open_writer_inner` to:

```rust
let mut db = open(...)?;
let expected = configure_writer_connection(&db, options)?;
verify_writer_connection(&db, expected)?;

let identity_state = inspect_identity(&db, identity)?;
let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;

initialize base schema / tags / digest or verify digest;
let initialized = initialize(&tx, is_new)?;

verify_writer_connection(&tx, expected)?;
tx.commit()?;
Ok((db, initialized))
```

After successful commit there must be no fallible Store/SQLite call before return.

### Why verify twice

- first verification proves the connection is safe before the initialization transaction;
- second verification ensures the kernel initializer did not leave required connection settings in a different state;
- any second-verification failure drops/rolls back the transaction.

Do not move `journal_mode=WAL` into the transaction. The mode transition is connection/database configuration and may require locking before `BEGIN IMMEDIATE`.

## 6. Identity inspection remains separate

Optionally extract the current identity queries as one private typed function:

```rust
enum DatabaseIdentityState {
    New,
    Existing,
}

fn inspect_database_identity(
    db: &Connection,
    identity: SchemaIdentity<'_>,
    create: bool,
) -> Result<DatabaseIdentityState, Error>;
```

This is useful only if it shortens `open_writer_inner`; do not create another public abstraction.

Preserve exact current semantics:

- missing file allowed only through `open_writer`;
- `open_existing_writer` never creates/initializes a new database;
- untagged empty or exact pre-tagged empty may initialize only in create mode;
- wrong application/user version rejects;
- schema digest remains exact.

## 7. Read-only connection

`open_reader` already sets `busy_timeout` before its pragma/identity queries. Do not rewrite it into the writer helper because its required settings differ:

```text
read-only flags
busy timeout
query_only=ON
exact application/user version
digest
```

A small shared `checked_timeout_ms` conversion may be reused by reader and writer so neither path can panic on oversized duration. Keep writer and reader postcondition functions separate.

## 8. Failure semantics

| Stage | Failure result | Durable bootstrap state |
|---|---|---|
| open / option conversion / pragma setup | Store error | no schema initialization by this call |
| identity inspection | exact schema/config error | unchanged |
| `BEGIN IMMEDIATE` | SQLite busy/error | unchanged |
| base schema / digest / initializer | Store or initializer error | rolled back |
| precommit postcondition verification | Store configuration error | rolled back |
| commit | SQLite commit error / outcome uncertain under SQLite semantics | no later code claims success |
| after commit | no fallible operations | success returned |

Do not catch `SQLITE_BUSY` and loop outside the configured SQLite busy handler. Do not translate all SQLite errors to schema mismatch.

## 9. Tests

### 9.1. Custom timeout applies before first database query

Create/tag a database, hold it under a second connection's exclusive lock, then call `open_existing_writer` with `busy_timeout=Duration::ZERO` on another thread.

Expected:

- call returns `SQLITE_BUSY` before blocker is released;
- it does not wait rusqlite's default five seconds;
- after releasing blocker and joining the thread, database identity is unchanged.

Use channels and bounded test deadlines; do not rely only on a fragile exact elapsed-millisecond assertion.

### 9.2. Initializer rollback

Initializer writes a sentinel then returns its own error.

Expected:

- open returns `OpenError::Initializer`;
- sentinel/base initialization is absent or the database remains in the exact allowed pre-initialized state;
- retry with successful initializer works.

### 9.3. Postcondition-before-commit fault injection

Use one private test seam for the postcondition function, not a product feature:

```rust
open_writer_inner_with_verifier(..., verify: impl Fn(&Connection) -> Result<(), Error>)
```

Production passes `verify_writer_connection`. Test verifier returns `DurabilityConfiguration` after initializer writes a sentinel.

Expected:

- function returns Store error;
- sentinel/schema tags/digest are not committed;
- no postcommit error path exists.

Keep this helper private and single-purpose. Do not expose callbacks in public `open_writer`.

### 9.4. Successful create/reopen

- create initializes exactly once;
- reopen calls initializer with `is_new=false`;
- exact schema digest preserved;
- writer reports requested busy timeout and required pragmas;
- read-only connection remains query-only.

### 9.5. Oversized timeout

An unrepresentable duration returns `InvalidBusyTimeout`; no panic and no database mutation.

## 10. Remove after migration

- late `db.busy_timeout(...)` call;
- post-commit `verify_writer_pragmas(&db)`;
- verifier that ignores expected busy timeout;
- any test-only public API;
- duplicate timeout conversion in reader/writer, if a single private helper is added.

No compatibility branch or old ordering remains.

## 11. Cross-PR ownership

- #26 compiler baseline may change unrelated compilation errors; do not copy fixes into R52.
- #63 owns DataRoot/state marker, not SQLite connection configuration.
- #71 owns optional supervisors, not Store bootstrap.
- #60 automation savepoints use the connection after successful open; no per-domain workaround for writer configuration belongs there.

This PR owns only `swarm-store` open/verification and the minimal kernel-host call-site tests needed to prove it.

## 12. Gate

After implementation:

```sh
cargo clippy --locked \
  -p swarm-store \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

Then the exact locked-database, rollback and reopen tests. Broad workspace tests remain final phase.

Handoff must report base/head SHA, reordered symbols, deleted postcommit path, test results and any remaining platform-specific lock qualification. This document does not change the database, pragmas or runtime behavior.