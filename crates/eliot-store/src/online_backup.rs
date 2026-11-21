//! A consistent SQLite snapshot while the daemon still owns the Store.
use super::{Store, sql_error};
use eliot_core::{Error, Result};
use rusqlite::{
    Connection, OpenFlags,
    backup::{Backup, StepResult},
};
use std::fs::OpenOptions;
use std::path::Path;
use std::time::{Duration, Instant};

impl Store {
    pub fn online_backup_bytes(&self) -> Result<u64> {
        let page_count: i64 = self
            .connection
            .pragma_query_value(None, "page_count", |row| row.get(0))
            .map_err(sql_error)?;
        let page_size: i64 = self
            .connection
            .pragma_query_value(None, "page_size", |row| row.get(0))
            .map_err(sql_error)?;
        page_count
            .checked_mul(page_size)
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| {
                Error::new(
                    "invalid_store",
                    "Store backup size exceeds the platform limit",
                )
            })
    }

    /// The caller owns a fresh private destination. The backup API includes
    /// committed WAL pages and writes a self-contained SQLite database.
    pub fn online_backup(&self, destination: &Path) -> Result<()> {
        self.ensure_committed_reads()?;
        #[cfg(windows)]
        let owned_path = eliot_core::verbatim_path(destination);
        #[cfg(windows)]
        let destination = owned_path.as_path();
        let mut target =
            Connection::open_with_flags(destination, OpenFlags::SQLITE_OPEN_READ_WRITE)
                .map_err(sql_error)?;
        // This temporary file is discarded after any failure. A destination
        // journal would consume additional space and is unnecessary.
        target
            .execute_batch("PRAGMA journal_mode=OFF;")
            .map_err(sql_error)?;
        {
            let backup = Backup::new(&self.connection, &mut target).map_err(sql_error)?;
            let deadline = Instant::now() + Duration::from_secs(120);
            loop {
                match backup.step(256).map_err(sql_error)? {
                    StepResult::Done => break,
                    StepResult::More => {}
                    _ => std::thread::sleep(Duration::from_millis(20)),
                }
                if Instant::now() >= deadline {
                    return Err(Error::new(
                        "backup_timeout",
                        "SQLite online backup did not finish within 120 seconds",
                    ));
                }
            }
        }
        target.close().map_err(|(_, error)| sql_error(error))?;
        OpenOptions::new()
            .write(true)
            .open(destination)
            .and_then(|file| file.sync_all())
            .map_err(|error| Error::new("platform_io", format!("sync Store backup: {error}")))
    }
}
