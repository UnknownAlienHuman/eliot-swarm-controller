# Current audit register

This is the active continuation register. AR-001–078 remain in the
[historical register](audit-register.md). Later ranges are frozen at their merge
checkpoints:

- AR-079–088 at [`b7a5fe1`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b7a5fe164a50cbefe2ca315c8f3a7f209d1c0d9b/docs/agent-operations/audit-current.md);
- AR-089–099 at [`e42c464`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/e42c464d58db770cc5490f46f391843f9fec8df2/docs/agent-operations/audit-current.md);
- AR-100–114 at [`e99c06b`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/e99c06bb490fe630eec70337ce5137a26e145d84/docs/agent-operations/audit-current.md).

[Audit index](audit-index.md) maps every range to immutable evidence;
[implementation](implementation.md) owns source changes;
[Store evolution](store-evolution.md) owns the target migration contract; and
[status](../implementation-status.md) owns landed behavior.

## Resume here

- Checkpoint: **2026-10-24 / Store identity, migrations and reader integrity**.
- Source inspected at **`e99c06bb490fe630eec70337ce5137a26e145d84`**.
- Connected chain:
  `swarm-store::{open_writer,open_existing_writer,open_reader}` →
  `store/mod.rs::{open_database,initialize_database,install_schema_extension}` →
  embedded `migrations/001–015` and special script/Operation-event installers →
  `doctor.rs` schema report → `status_reader.rs` query-only WAL reads →
  restart, partial migration, corruption and downgrade boundaries.
- Overall audit: **incomplete, 126 entries**. AR-115–126 are recorded below; prior
  dispositions remain in immutable checkpoints.
- Preserved in this pass: DataRoot/single-writer ownership; caller timeout before the
  first SQLite query; foreign identity validation before persistent WAL mutation;
  `BEGIN IMMEDIATE`; initializer and connection postconditions before commit; no
  fallible post-commit validation; read-only/query-only status connection; committed
  WAL snapshots while the writer is held; existing-only recovery open.
- Open in this pass: application `user_version` remains `1` while twelve later schema
  fragments are installed by source SHA markers; an older binary can accept a newer
  extension set; harmless base-SQL text changes can reject a compatible database;
  generic extensions trust marker presence without live-shape verification; special
  extensions use incompatible reference-DB/DDL-token mechanisms; deleted products have
  no numeric removal migration; Doctor and readers know only the base digest; no
  bounded migration integrity check exists.
- **Next exact chain:** IPC/MCP concurrency and hot reads:
  `swarm-client::Client` request multiplexing → MCP connection mutex and profile facade
  → Store call/read routing → subscription poller/forwarder queues → exact
  `tasks/get`, mailbox and Operation reads → disconnect, slow consumer and writer
  occupancy. Preserve Store readback authority while removing serialized frontend
  calls and duplicate wake logs.
- Runtime implementation order remains **A1b–A3, then B–V**. Source-audit order does
  not reorder implementation.
- PR #188 is merged at `e99c06b`. Exact head `0a5a099`, workflow run `40087522775`,
  passed changed-path classification, documentation/rustfmt, all-target compile and
  changed-line Clippy on Ubuntu and Windows plus selected IPC/Muse/Command boundaries.
  Broad/live qualification was not run.
- Still unqualified: actual legacy-database migration, future-version rejection under
  retained files, interrupted migration/power loss, bounded integrity checks, database
  corruption classes, large data migration cost and installed autonomous operation.

On resume, compare live `main` with the inspected head, reopen only changed owners and
consumers, then continue the named IPC/MCP chain. Documentation is not a migration.

<a id="findings-ar-115126"></a>
## Findings AR-115–126

| ID | Requirement / path | Disposition | Remaining boundary |
| --- | --- | --- | --- |
| <a id="ar-115"></a>AR-115 | Writer bootstrap and truthful success | **Preserve.** `swarm-store` applies caller timeout before identity queries, rejects foreign identity before WAL, configures the accepted DB, enters `BEGIN IMMEDIATE`, runs initialization/postconditions and commits with no later fallible check. | Keep this exact order while replacing schema identity. WAL may never mutate a rejected foreign database. |
| <a id="ar-116"></a>AR-116 | Numeric schema version | **Open.** Every writer, recovery writer and reader requires `user_version=1`. Files `002–015` do not advance it; their prefixes are documentary, not application migration authority. | One current numeric version and a closed ordered migration plan. Readers require the current version; writers migrate only known older versions. |
| <a id="ar-117"></a>AR-117 | Downgrade/future-version fence | **Open correctness defect.** A newer binary can add extension tables/indexes/triggers while leaving base digest and `user_version=1` unchanged. An older binary knows neither the markers nor objects and can reopen/mutate the newer database. | Every compatible schema change advances `user_version`; reject a future version before WAL or any mutation. |
| <a id="ar-118"></a>AR-118 | Base SQL digest | **Open false compatibility boundary.** Existing DBs must match SHA-256 of the exact bundled `001_core.sql` bytes. Comment/whitespace/source refactoring can reject an otherwise identical live schema, while extra newer objects are ignored. | Remove source-text digest as compatibility authority after a finite legacy bridge. Use application ID, numeric version and semantic postconditions. |
| <a id="ar-119"></a>AR-119 | Generic extension verification | **Open.** On a matching `schema_extension:*` marker, `install_schema_extension` returns without checking required tables/indexes/triggers. A dropped or changed live object can pass startup until a later query fails. Migration 015 supplies an empty preexisting-object list. | Migration-specific bounded postconditions at apply/open boundaries; never trust a mutable marker alone and never adopt conflicting objects. |
| <a id="ar-120"></a>AR-120 | Special extension verification | **Open inconsistency/excess.** Operation-event installers compare live triggers with disposable reference DBs; script event scope additionally rebuilds a reference DB, inspects PRAGMAs/indexes/FKs and tokenizes DDL. Generic extensions perform none of this. | One bounded postcondition model. Preserve required columns/FKs/indexes/triggers and data invariants, not canonicalized source DDL. Delete reference-DB/token machinery after transition. |
| <a id="ar-121"></a>AR-121 | Atomic installation and rollback | **Preserve.** Base creation, all current extension installation, bootstrap metadata and kernel initialization occur in one immediate transaction; callback or precommit failure rolls back. Status reader starts only after the writer is ready. | Ordered numeric migrations must retain one truthful transaction/commit boundary and expose no partially migrated DB to readers. |
| <a id="ar-122"></a>AR-122 | Schema retirement | **Open.** Current installers add or verify objects but provide no ordered path to migrate retained data and drop tables/indexes/triggers for removed products. Deleting coordination/runtime code would leave its storage and markers indefinitely. | Each product cut includes concrete data disposition and a numeric removal migration in the same vertical slice. No disabled compatibility storage. |
| <a id="ar-123"></a>AR-123 | Reader and Doctor authority | **Open.** `open_reader` checks only application ID, `user_version=1` and base digest. Doctor repeats the base SQL digest comparison and can report no extension-specific damage that generic startup also misses. | Writer completes current-version migrations first; readers accept exact current version/query-only; Doctor reports numeric version, migration/postcondition gaps and bounded integrity facts. |
| <a id="ar-124"></a>AR-124 | Integrity/corruption checks | **Open.** Selected shape verifiers exist, but no common bounded `quick_check`/foreign-key/postcondition pass covers a completed migration. Search found no `quick_check`, `integrity_check` or `foreign_key_check` path. | Bounded checks at migration/qualification boundaries with explicit incomplete/error results; ordinary startup must not become an unbounded full-database scan. |
| <a id="ar-125"></a>AR-125 | Legacy v1 transition | **Open finite migration.** Existing databases encode current physical state through base digest plus many marker digests, including data-bearing migration 013. No single classifier enumerates which concrete legacy combinations are accepted and how required data is retained. | One finite v1 bridge recognizes only supported concrete marker/object states, verifies retained data, migrates to the current numeric version and removes obsolete digest authority. Unknown combinations fail visibly. |
| <a id="ar-126"></a>AR-126 | Schema ownership | **Open fragmentation.** `swarm-store` owns exact base bytes/version stamping; kernel `initialize_database` owns extension order/markers; three special modules own private verifiers; Doctor owns another identity check. No component owns one complete current schema transition. | `swarm-store` owns connection/transaction mechanics; the kernel owns one ordered domain migration plan and bounded postconditions; Doctor/readers consume the same numeric authority. |

<a id="trace-2026-10-24-store-evolution"></a>
## Source trace at e99c06b: Store evolution

Inspected 2026-10-24. This is source evidence, not an executed database migration or
corruption qualification.

### AR-115 — preserve the repaired open order

`swarm-store::open_writer_inner` already has a valuable boundary established by the R52
repair:

```text
validate options/identity
-> open connection
-> install caller busy timeout before the first query
-> inspect application ID, user version and emptiness
-> only after the DB is accepted: foreign_keys, WAL, FULL
-> BEGIN IMMEDIATE
-> schema initialization/verification and kernel callback
-> verify connection postconditions through the transaction
-> COMMIT
-> direct success return
```

This prevents a foreign SQLite file from being changed to WAL before rejection and
prevents startup from returning failure after durable bootstrap. Do not regress it
while introducing migrations. `open_existing_writer` also refuses to create a missing
recovery database. `status_reader` opens read-only/query-only and can read the last
committed WAL snapshot while a writer transaction is held.

### AR-116–118 — version 1 is not the current schema version

`SchemaIdentity` receives `application_id`, `user_version` and bundled base SQL.
Kernel callers pass `user_version: 1` for normal writer, recovery writer and reader.
New databases execute `001_core.sql`, stamp version 1 and save SHA-256 of those exact
bytes. Existing databases must match all three.

After that, `initialize_database` applies workspace, owned-service, scripts, GitHub,
GitHub effect, GM fencing, read-position, provider-condition and mailbox fragments plus
three special installers. None advances `user_version`. Therefore:

- a formatting/comment change to `001_core.sql` can reject compatible live DDL;
- the base digest says nothing about extension objects;
- an old binary still sees application ID + version 1 + unchanged base digest after a
  newer binary added an extension, and can open the database.

The target uses `application_id` for database family and `user_version` for the complete
application schema. A future version is rejected before WAL/configuration; a supported
older version enters the migration transaction.

### AR-119–120 — markers either prove too little or far too much

Generic `install_schema_extension` hashes SQL text. With no marker it checks selected
object names are absent, executes the SQL and writes the marker. With a matching marker
it performs no live verification. This is insufficient for data-bearing migration 013:
a retained marker does not prove its triggers/indexes or imported read-position rows
still exist. Migration 015 passes no protected object names before executing expression
indexes.

The special installers take unrelated approaches:

- failure/cancel event installers create a disposable reference DB and compare exact
  trigger rows;
- script event scope verifies marker digests, creates a reference DB, compares schema
  objects, `table_xinfo`, foreign keys and indexes, and canonicalizes DDL through a
  private tokenizer.

The useful requirement is semantic: required objects/columns/FKs/indexes/triggers and
retained data invariants must hold. Exact SQL source and a custom SQL lexer are not the
runtime compatibility contract.

### AR-121–122 — transactionality is useful; evolution/removal is missing

All extension setup currently runs inside the same immediate startup transaction as
bootstrap metadata. A failure rolls back marker and DDL/data changes together, and the
reader starts after commit. Preserve this.

However, the system has no reverse path. There is no versioned migration that can:

```text
classify concrete retained rows
-> migrate/export/dispose them
-> remove old indexes/triggers/tables
-> advance the application version
```

This blocks honest deletion of mailbox identity indexes, coordination products,
workflow ledgers and retired executors. Keeping obsolete storage forever is not a safe
migration strategy.

### AR-123–124 — readback reports source identity, not complete database health

`open_reader` validates only the base identity triple. Doctor embeds `001_core.sql`
again, compares the same digest and emits `SCHEMA_DIGEST_MISMATCH`; it does not report a
single current migration version or common extension postconditions. Some startup
installers detect selected object damage, others do not.

No bounded common integrity path was found. The target does not run an unbounded full
integrity scan on every ordinary open. It runs explicit migration postconditions and
bounded integrity checks, records gaps, and keeps broad corruption/load qualification
separate.

### AR-125–126 — finite legacy bridge, one owner

The existing version-1 population must be handled once, not supported forever through
every marker implementation. The bridge should:

1. verify application ID and reject foreign/nonempty untagged files;
2. classify only known supported v1 marker/object combinations;
3. preserve concrete required rows and reject ambiguous/damaged state;
4. run ordered migrations in the immediate transaction;
5. stamp one current numeric version;
6. remove base/extension digest authority after no current reader needs it;
7. expose bounded semantic postconditions to Doctor/readers.

`swarm-store` remains a small connection/transaction package. It must not absorb domain
SQL or become an ORM. Kernel Store owns the ordered domain migration list; special
products no longer install themselves independently at startup.

## Inspected source boundaries

| Source | Read boundary |
| --- | --- |
| `crates/swarm-store/src/lib.rs` | Writer/reader identity, timeout, WAL, immediate transaction, base digest and version stamping. |
| `store/mod.rs` | Normal/recovery open, bootstrap, extension ordering/markers and generic installer. |
| `migrations/001_core.sql`, `002–015` selected files | Base and extension DDL/data transforms; prefixes currently do not advance application version. |
| `script_event_schema.rs` | Digest chain, reference DB, PRAGMA/index/FK comparison and private DDL tokenizer. |
| `operation_{failure,cancel}_event_schema.rs` | Trigger-specific reference-schema validation. |
| `doctor.rs` | Base digest/version/PRAGMA reporting and read-only diagnostics. |
| `status_reader.rs` | Current exact base identity, query-only reader, WAL snapshot and principal revalidation. |
| remediation R52 | Historical proof of corrected open/commit ordering and its remaining unexecuted scenarios. |

Selected paths, not whole-project certification. Actual migration cost, crash/power-loss,
foreign-file preservation, future-version rejection and corruption matrices remain
unexecuted.

## Next work

Continue the IPC/MCP chain named in [Resume here](#resume-here). Runtime Store work is
owned by [Store evolution](store-evolution.md): first connect numeric authority and the
finite v1 bridge, then migrate product storage as each vertical deletion slice lands.
