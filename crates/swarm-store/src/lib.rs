//! Kernel-local SQLite opening, schema identity, and transaction primitives.
//!
//! This crate owns no Task, authorization, receipt, or provider policy. The
//! kernel supplies immutable schema bytes and its initialization callback.
//! The existing kernel's writer thread remains the one authoritative writer;
//! call `open_writer` there while retaining the DataRoot lock in that thread.

use std::{path::Path, time::Duration};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use sha2::{Digest, Sha256};

pub type Result<T, E> = std::result::Result<T, E>;

pub const PROJECT_MIN_SQLITE_VERSION: i32 = 3_051_003;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SQLite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("stored JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("database identity or supplied base schema is invalid")]
    InvalidIdentity,
    #[error("not this prototype's version-1 database; no automatic overwrite or downgrade")]
    SchemaMismatch,
    #[error("migration content differs; refusing to open a draft/reference database")]
    SchemaDigestMismatch,
    #[error("read-only status connection does not match the initialized store")]
    ReaderSchemaMismatch,
    #[error("SQLite runtime version is below the configured minimum ({actual} < {minimum})")]
    SqliteVersion { actual: i32, minimum: i32 },
    #[error("query-only mode could not be enabled for the status connection")]
    QueryOnlyUnavailable,
    #[error("SQLite foreign-key, WAL, or FULL-sync settings were not applied")]
    DurabilityConfiguration,
}

impl Error {
    /// Stable compatibility code for the current kernel error envelope.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Sqlite(_) => "STORE_ERROR",
            Self::Json(_) => "INVALID_PARAMS",
            Self::InvalidIdentity | Self::QueryOnlyUnavailable | Self::DurabilityConfiguration => {
                "STORE_CONFIGURATION"
            }
            Self::SchemaMismatch | Self::SchemaDigestMismatch | Self::ReaderSchemaMismatch => {
                "SCHEMA_MISMATCH"
            }
            Self::SqliteVersion { .. } => "SQLITE_VERSION",
        }
    }

    /// Message for the current kernel RPC error envelope. This avoids adding
    /// the package's contextual Display prefixes to the existing SQLite
    /// error text while preserving the established store diagnostics.
    pub fn message(&self) -> String {
        match self {
            Self::Sqlite(error) => error.to_string(),
            Self::Json(error) => error.to_string(),
            Self::InvalidIdentity => "database identity or base schema is invalid".to_owned(),
            Self::SchemaMismatch => {
                "not this prototype's version-1 database; no automatic overwrite or downgrade"
                    .to_owned()
            }
            Self::SchemaDigestMismatch => {
                "migration content differs; refusing to open a draft/reference database".to_owned()
            }
            Self::ReaderSchemaMismatch => {
                "read-only status connection does not match the initialized store".to_owned()
            }
            Self::SqliteVersion { minimum, .. } if *minimum == PROJECT_MIN_SQLITE_VERSION => {
                "bundled SQLite >= 3.51.3 is required".to_owned()
            }
            Self::SqliteVersion { actual, minimum } => {
                format!(
                    "SQLite runtime version is below the configured minimum ({actual} < {minimum})"
                )
            }
            Self::QueryOnlyUnavailable => "status connection is not query-only".to_owned(),
            Self::DurabilityConfiguration => "foreign_keys/WAL/FULL were not applied".to_owned(),
        }
    }
}

/// Root-owned schema identity. The schema bytes stay in the kernel package and
/// are passed here unchanged; this crate does not copy or own SQL migrations.
#[derive(Debug, Clone, Copy)]
pub struct SchemaIdentity<'a> {
    pub application_id: i64,
    pub user_version: i64,
    pub base_schema: &'a str,
}

impl SchemaIdentity<'_> {
    fn validate(&self) -> Result<(), Error> {
        if self.application_id == 0 || self.user_version <= 0 || self.base_schema.is_empty() {
            return Err(Error::InvalidIdentity);
        }
        Ok(())
    }

    fn digest(&self) -> String {
        format!("{:x}", Sha256::digest(self.base_schema.as_bytes()))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WriterOptions {
    pub minimum_sqlite_version: i32,
    pub busy_timeout: Duration,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            minimum_sqlite_version: PROJECT_MIN_SQLITE_VERSION,
            busy_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReaderOptions {
    pub busy_timeout: Duration,
}

impl Default for ReaderOptions {
    fn default() -> Self {
        Self {
            busy_timeout: Duration::from_secs(5),
        }
    }
}

/// The initialization callback error remains kernel-owned; SQLite/opening
/// errors remain `swarm_store::Error` so callers can map the two separately.
#[derive(Debug)]
pub enum OpenError<E> {
    Store(Error),
    Initializer(E),
}

impl<E> From<Error> for OpenError<E> {
    fn from(value: Error) -> Self {
        Self::Store(value)
    }
}

impl<E> From<rusqlite::Error> for OpenError<E> {
    fn from(value: rusqlite::Error) -> Self {
        Self::Store(Error::Sqlite(value))
    }
}

/// Open and initialize one writable SQLite connection. The caller controls
/// the thread which owns the returned connection. Creation of the base schema,
/// application/user version, schema digest, and all kernel-specific bootstrap
/// work occur inside one `IMMEDIATE` transaction.
pub fn open_writer<T, E>(
    path: &Path,
    identity: SchemaIdentity<'_>,
    options: WriterOptions,
    initialize: impl FnOnce(&Transaction<'_>, bool) -> Result<T, E>,
) -> Result<(Connection, T), OpenError<E>> {
    identity.validate()?;
    if options.minimum_sqlite_version > 0 {
        let actual = rusqlite::version_number();
        if actual < options.minimum_sqlite_version {
            return Err(Error::SqliteVersion {
                actual,
                minimum: options.minimum_sqlite_version,
            }
            .into());
        }
    }

    let mut db = Connection::open(path)?;
    let version: i64 = db.pragma_query_value(None, "user_version", |row| row.get(0))?;
    let application_id: i64 = db.pragma_query_value(None, "application_id", |row| row.get(0))?;
    let empty: bool = db.query_row(
        "SELECT COUNT(*) = 0 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    let untagged_empty = empty && version == 0 && application_id == 0;
    let already_tagged_empty =
        empty && application_id == identity.application_id && version == identity.user_version;
    let is_new = untagged_empty || already_tagged_empty;
    if !is_new && (application_id != identity.application_id || version != identity.user_version) {
        return Err(Error::SchemaMismatch.into());
    }

    db.pragma_update(None, "foreign_keys", "ON")?;
    db.pragma_update(None, "journal_mode", "WAL")?;
    db.pragma_update(None, "synchronous", "FULL")?;
    db.busy_timeout(options.busy_timeout)?;

    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let digest = identity.digest();
    if is_new {
        tx.execute_batch(identity.base_schema)?;
        tx.pragma_update(None, "application_id", identity.application_id)?;
        tx.pragma_update(None, "user_version", identity.user_version)?;
        write_schema_digest(&tx, &digest)?;
    } else {
        verify_schema_digest(&tx, &digest)?;
    }
    let initialized = initialize(&tx, is_new).map_err(OpenError::Initializer)?;
    tx.commit()?;
    verify_writer_pragmas(&db)?;
    Ok((db, initialized))
}

/// Open a second, explicitly read-only status connection. It never creates a
/// missing database and is bound to the exact base schema identity supplied
/// by the kernel. Authorization and status projection remain kernel duties.
pub fn open_reader(
    path: &Path,
    identity: SchemaIdentity<'_>,
    options: ReaderOptions,
) -> Result<Connection, Error> {
    identity.validate()?;
    let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    db.busy_timeout(options.busy_timeout)?;
    db.pragma_update(None, "query_only", "ON")?;

    let query_only: i64 = db.pragma_query_value(None, "query_only", |row| row.get(0))?;
    let application_id: i64 = db.pragma_query_value(None, "application_id", |row| row.get(0))?;
    let user_version: i64 = db.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if query_only != 1 {
        return Err(Error::QueryOnlyUnavailable);
    }
    if application_id != identity.application_id || user_version != identity.user_version {
        return Err(Error::ReaderSchemaMismatch);
    }
    verify_reader_schema_digest(&db, &identity.digest())?;
    Ok(db)
}

#[derive(Debug, Clone, Copy)]
pub enum TransactionMode {
    Deferred,
    Immediate,
    Exclusive,
}

#[derive(Debug)]
pub enum TransactionError<E> {
    Store(Error),
    Operation(E),
}

/// Run a closure in a bounded transaction. An operation error drops the
/// transaction and rolls it back; commit errors remain distinct from domain
/// errors so the kernel can preserve its existing error envelope.
pub fn with_transaction<T, E>(
    db: &mut Connection,
    mode: TransactionMode,
    operation: impl FnOnce(&Transaction<'_>) -> Result<T, E>,
) -> Result<T, TransactionError<E>> {
    let behavior = match mode {
        TransactionMode::Deferred => TransactionBehavior::Deferred,
        TransactionMode::Immediate => TransactionBehavior::Immediate,
        TransactionMode::Exclusive => TransactionBehavior::Exclusive,
    };
    let tx = db
        .transaction_with_behavior(behavior)
        .map_err(Error::Sqlite)
        .map_err(TransactionError::Store)?;
    let value = operation(&tx).map_err(TransactionError::Operation)?;
    tx.commit()
        .map_err(Error::Sqlite)
        .map_err(TransactionError::Store)?;
    Ok(value)
}

fn write_schema_digest(tx: &Transaction<'_>, digest: &str) -> Result<(), Error> {
    let canonical_json = format!("\"{digest}\"");
    tx.execute(
        "INSERT INTO meta(key,value_json) VALUES('schema_digest',?1)",
        params![canonical_json],
    )?;
    Ok(())
}

fn verify_schema_digest(db: &Connection, expected: &str) -> Result<(), Error> {
    if read_schema_digest(db)? != Some(serde_json::json!(expected)) {
        return Err(Error::SchemaDigestMismatch);
    }
    Ok(())
}

fn verify_reader_schema_digest(db: &Connection, expected: &str) -> Result<(), Error> {
    if read_schema_digest(db)? != Some(serde_json::json!(expected)) {
        return Err(Error::ReaderSchemaMismatch);
    }
    Ok(())
}

fn read_schema_digest(db: &Connection) -> Result<Option<serde_json::Value>, Error> {
    let stored: Option<String> = db
        .query_row(
            "SELECT value_json FROM meta WHERE key='schema_digest'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    stored
        .map(|raw| serde_json::from_str(&raw).map_err(Error::Json))
        .transpose()
}

fn verify_writer_pragmas(db: &Connection) -> Result<(), Error> {
    let foreign_keys: i64 = db.pragma_query_value(None, "foreign_keys", |row| row.get(0))?;
    let journal_mode: String = db.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    let synchronous: i64 = db.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    if foreign_keys != 1 || journal_mode != "wal" || synchronous != 2 {
        return Err(Error::DurabilityConfiguration);
    }
    Ok(())
}
