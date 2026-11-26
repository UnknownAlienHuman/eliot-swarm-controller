//! Reusable read connections. Each call owns a fresh SQLite snapshot.
use super::*;
use std::{path::PathBuf, sync::Mutex};

pub struct ProjectionReaders {
    path: PathBuf,
    idle: Mutex<Vec<Store>>,
}

impl ProjectionReaders {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            idle: Mutex::new(Vec::new()),
        }
    }

    pub fn read<T>(&self, project: impl FnOnce(&Store) -> Result<T>) -> Result<T> {
        let cached = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop();
        let store = match cached {
            Some(store) => store,
            None => {
                let store = Store::open_read_only(&self.path)?;
                store
                    .connection
                    .execute_batch("ROLLBACK; PRAGMA cache_size = -8192;")
                    .map_err(sql_error)?;
                store.connection.set_prepared_statement_cache_capacity(128);
                store
            }
        };
        let snapshot = store
            .connection
            .unchecked_transaction()
            .map_err(sql_error)?;
        // Source and workspace views repeatedly resolve the same Agent.
        // Reuse that row only inside this read transaction.
        *store.snapshot_agent.borrow_mut() = Some(SnapshotAgentCache::default());
        let result = project(&store);
        *store.snapshot_agent.borrow_mut() = None;
        // End the snapshot before returning the connection to the pool.
        // A failed rollback drops the connection rather than reusing it.
        snapshot.rollback().map_err(sql_error)?;
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if idle.len() < 4 {
            idle.push(store);
        }
        result
    }
}
