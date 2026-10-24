# Store evolution — one numeric schema authority

This document defines the target database identity, migration and reader contract.
[Implementation I](implementation.md#i--store-evolution) owns source changes;
[current audit](audit-current.md#findings-ar-115126) records evidence; and
[status](../implementation-status.md) records landed behavior.

The target is deliberately small. `swarm-store` remains connection/transaction
mechanics. Kernel Store owns domain migrations. There is no ORM, schema registry
service, generic repository layer or compatibility database.

## 1. Product law

```text
application_id identifies an Eliot database family
user_version identifies the complete application schema
one closed ordered migration plan owns every accepted transition
one IMMEDIATE transaction owns migration, postconditions and version stamp
readers open only the committed current version
```

A SQL file hash, comment, filename, extension marker or reference database is not the
application schema version. A future database version is rejected before WAL or any
other persistent mutation. A damaged current version is reported as damaged, not
silently adopted or recreated.

## 2. Preserve the current open boundary

The R52 repair established the correct writer order. Numeric migrations must retain it:

```text
validate options and expected database family
-> open SQLite connection
-> install caller busy timeout before the first query
-> read application_id, user_version and emptiness
-> reject foreign/future/unsupported state before persistent DB configuration
-> for an accepted Eliot DB: foreign_keys=ON, journal_mode=WAL, synchronous=FULL
-> BEGIN IMMEDIATE
-> create or migrate through the closed plan
-> run bounded postconditions and required integrity checks
-> stamp the current user_version inside the transaction
-> COMMIT
-> return success with no fallible post-commit validation
```

Keep:

- DataRoot lock and one durable writer owner;
- caller timeout from the first SQLite query;
- no WAL mutation of a rejected foreign database;
- rollback-on-error for initializer, migration and postcondition failure;
- no retry loop around `SQLITE_BUSY` beyond SQLite's configured busy handler;
- existing-only recovery open that never creates a replacement database;
- read-only/query-only status connection after writer readiness;
- WAL readers observing committed state without waiting behind an uncommitted writer.

## 3. Numeric identity

Use one kernel-owned constant:

```text
APPLICATION_ID = existing Eliot family identifier
CURRENT_SCHEMA_VERSION = 2   # first consolidated numeric baseline
```

Version meanings:

- `0` plus an empty schema: new database candidate;
- `1`: legacy marker/digest database requiring the finite bridge below;
- `2`: first consolidated current schema with numeric authority;
- `>2`: future ordered migrations, one version at a time;
- `> CURRENT_SCHEMA_VERSION`: reject as a future database;
- wrong nonzero `application_id`: reject as foreign;
- nonempty schema with zero/wrong identity: reject, never adopt.

Historical filenames `002–015` describe the old extension sequence. They do **not**
become target `user_version` values merely because of their prefixes.

A new database is created directly as the complete current version inside one
transaction. It need not expose an intermediate legacy version.

## 4. Ownership and API boundary

### `swarm-store`

Own only:

- SQLite open flags and minimum SQLite version;
- timeout configuration;
- pre-WAL identity read;
- writer PRAGMAs;
- `BEGIN IMMEDIATE` and commit/rollback boundary;
- read-only/query-only reader mechanics;
- typed classification of new/current/older/future/foreign state.

It does not own domain SQL, Task/Operation policy or product-specific table knowledge.
Remove `base_schema` bytes and SHA-256 from the long-term `SchemaIdentity` API.

A compact target shape is:

```rust
DatabaseIdentity {
    application_id,
    current_user_version,
}

OpenedDatabase {
    observed_user_version,
    is_empty,
}
```

Exact Rust names are implementation details. The key boundary is that the kernel sees
the observed supported version inside the writer transaction and applies its own closed
plan.

### Kernel Store

Own:

- the ordered migration table;
- domain SQL/data transforms;
- migration-specific preconditions and postconditions;
- the finite version-1 bridge;
- current schema creation;
- explicit product-storage retirement.

### Doctor and readers

Consume the same numeric authority. They do not independently hash SQL or reconstruct a
reference schema.

## 5. Closed migration plan

Use an explicit ordered table, not filename discovery or a scripting framework:

```text
Migration {
  from_version
  to_version
  apply(transaction)
  verify(transaction)
}
```

Rules:

1. `to_version` is exactly the next supported application version.
2. Every version has at most one outgoing migration.
3. The complete path to `CURRENT_SCHEMA_VERSION` is known before any migration write.
4. Unknown older versions fail before mutation.
5. All pending steps for one startup run in the existing immediate transaction.
6. Each step verifies its own semantic postconditions before the version advances.
7. The final current-version postconditions run before commit.
8. A failure rolls back data, DDL and version changes together.
9. A retry starts from the last committed version and does not replay an external/native
   effect; migrations operate only on the local Store transaction.

Do not implement a generic migration DSL. A small Rust array plus ordinary SQL/functions
is sufficient.

## 6. Finite version-1 bridge

Version 1 is not a permanent compatibility mode. It is a finite input format whose
physical state is currently encoded by:

- the base `schema_digest`;
- `schema_extension:*` markers;
- selected tables, indexes and triggers;
- data-bearing transforms such as read-position imports;
- special script/Operation-event verifier state.

The `1 -> 2` bridge must:

1. verify application identity and the exact supported legacy family;
2. classify known marker/object combinations before changing them;
3. reject unknown, contradictory or damaged combinations;
4. enumerate concrete retained rows affected by a transform/removal;
5. preserve required Tasks, Attempts, Operations, artifacts, results, reviews and
   active resource/recovery facts;
6. migrate or explicitly dispose obsolete product state;
7. install the current semantic objects;
8. run bounded postconditions;
9. delete base/extension digest authority after no current reader uses it;
10. stamp version 2 and commit once.

Do not build a universal converter for every historical source checkout. Support the
concrete legacy state that can exist in retained user databases. Anything else remains a
visible migration error requiring an explicit export/recovery decision.

## 7. Semantic postconditions

A postcondition proves the behavior needed by current code, not byte equality with a SQL
file. Depending on the migration, check a bounded subset such as:

- required table/index/trigger existence and object kind;
- required columns, primary-key/nullability shape and selected strictness;
- required foreign-key edges;
- selected unique/partial index columns and predicate owner;
- trigger presence plus the specific retained transition it must produce;
- row-count/equality invariants for a bounded transform;
- absence of an obsolete object after a deletion migration;
- no unresolved legacy marker after the v1 bridge;
- exact current `user_version` before commit.

Use direct PRAGMA/table-valued-PRAGMA reads and small SQL assertions. Do not canonicalize
entire `sqlite_schema.sql`, tokenize arbitrary SQL or construct a disposable reference
DB on every ordinary startup.

## 8. Integrity and corruption

Migration verification and broad database integrity are different.

### Migration-time bounded checks

For a migration that can affect relationships or rebuild data, run bounded checks such
as:

- migration-specific row invariants;
- bounded foreign-key violation readback;
- bounded `quick_check` result where appropriate;
- required object/column/index/trigger postconditions.

A non-`ok` or incomplete result fails the migration and rolls back. The error reports
which check failed without echoing private payload data.

### Ordinary open

Ordinary current-version startup verifies the small current postcondition set required
for safe operation. It does not scan all historical rows or rebuild a reference DB.

### Qualification/Doctor

Doctor reports:

```text
application_id
observed/current user_version
migration required | current | future | damaged
last completed migration if retained diagnostically
bounded postcondition/integrity gaps
reader/writer PRAGMAs
```

Doctor mutates nothing. Broad `integrity_check`, large-table verification and corruption
matrix testing remain explicit qualification, not every startup.

## 9. Reader contract

The status/WAL reader:

1. opens existing file read-only;
2. applies bounded busy timeout and `query_only=ON`;
3. verifies Eliot `application_id`;
4. requires exact `CURRENT_SCHEMA_VERSION`;
5. verifies the compact current-reader postconditions needed by its queries;
6. never runs a migration or writes a marker.

The writer completes migration before the reader starts. A reader cannot observe an
intermediate version. A future/older/damaged database fails closed rather than opening a
partial status surface.

## 10. Recovery writer

Lifecycle recovery remains existing-only and narrow:

- never creates a missing database;
- requires Eliot application ID and exact current version;
- runs no migration;
- writes only the retained host-lifecycle fact under the existing DataRoot lock;
- cannot bypass a migration/postcondition failure from normal startup.

A recovery path is not an alternate schema installer.

## 11. Product deletion and retained data

Every vertical product deletion names its Store migration:

```text
inventory concrete active/unresolved rows
-> migrate required facts to retained owner or record explicit disposition
-> stop new writes in the old format
-> delete old callers/readers/public surfaces
-> drop old indexes/triggers/tables
-> remove legacy markers/backfills
-> advance user_version
```

Examples include mailbox JSON identity indexes, coordination/Concilium/watch products,
workflow ledgers and retired executor-specific state. Completed Operations/artifacts can
remain immutable history without keeping an executable table family when normal reads
already project them through retained owners.

Never drop a table because its Rust module was removed. Conversely, never retain a table
forever merely because historical rows once existed.

## 12. Implementation sequence

### I0 — numeric mechanics

- replace exact base-schema identity in `swarm-store` with observed/current numeric
  classification;
- preserve R52 timeout/identity/WAL/transaction ordering;
- reject future versions before persistent mutation;
- keep readers exact-current and query-only.

### I1 — current schema and v1 bridge

- define `CURRENT_SCHEMA_VERSION = 2`;
- create new databases directly at v2;
- implement the closed v1 classifier/bridge;
- preserve concrete retained data and reject unknown combinations;
- stop writing new digest/extension markers.

### I2 — bounded postconditions and Doctor

- replace generic marker trust with current semantic checks;
- replace Doctor's base digest report with numeric migration/integrity state;
- delete reference-DB and DDL-token machinery after the bridge no longer needs it.

### I3 — product migrations

- attach a numeric migration to each mailbox/coordination/workflow/executor deletion;
- migrate live required rows and drop obsolete storage in the same slice.

### I4 — remove legacy authority

- remove `schema_digest`, `schema_extension:*` compatibility readers/writers and old
  migration-install helpers after all supported v1 databases have a connected bridge;
- remove `sha2` from `swarm-store` if no retained mechanic uses it;
- leave historical migration SQL only where tests/bridge evidence still require it.

## 13. Failure semantics

| Input/state | Required result |
| --- | --- |
| Empty new DB | Create complete current schema atomically and stamp v2. |
| Current v2 DB | Verify compact current postconditions; no schema writes. |
| Supported v1 DB | Run finite bridge in one immediate transaction. |
| Unknown older version | Reject before migration writes. |
| Future version | Reject before WAL/persistent mutation. |
| Wrong application ID | Reject as foreign before WAL/persistent mutation. |
| Nonempty untagged DB | Reject; never adopt. |
| Migration SQL/data failure | Roll back DDL/data/version; old committed version remains. |
| Postcondition/integrity failure | Roll back and report exact bounded gap. |
| Missing current object | Report damaged schema; do not recreate silently under the same version. |
| Reader sees non-current version | Fail closed; reader never migrates. |
| Recovery open sees non-current version | Fail closed; recovery never migrates. |

## 14. Verification

Minimum connected proofs:

- foreign SQLite file is rejected without changing journal mode;
- future Eliot version is rejected before persistent mutation;
- new database is created once at v2 and reopens without schema writes;
- supported v1 database migrates exactly once and retains selected domain rows;
- unknown/contradictory v1 marker/object combination fails visibly;
- injected migration and postcondition failures roll back data, DDL and version;
- crash before commit leaves the old committed version and no partial objects;
- query-only reader cannot write or open old/future/mid-migration state;
- status reader still returns committed WAL state while a writer transaction is held;
- missing required current object is detected without full DDL comparison;
- product deletion migrates/disposes concrete live rows before dropping storage;
- no base SQL digest, extension digest, reference-DB or DDL-token compatibility path
  remains after I4;
- no post-commit validation can turn a committed migration into startup failure.

During construction run rustfmt and minimal affected warnings-denied compile/Clippy.
Focused migration fixtures close each vertical path. Broad corruption, power-loss,
large-data and installed-product qualification follows the completed Store transition.
