use serde::Serialize;
pub use swarm_contracts::error::{NativeHttpFailure, NativeRpcRejectionClass};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Serialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct Error {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejection_class: Option<NativeRpcRejectionClass>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_http_failure: Option<NativeHttpFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secondary_codes: Vec<String>,
}

impl Error {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            rejection_class: None,
            native_http_failure: None,
            secondary_codes: Vec::new(),
        }
    }
    pub fn with_rejection_class(mut self, rejection_class: NativeRpcRejectionClass) -> Self {
        self.rejection_class = Some(rejection_class);
        self
    }
    pub fn with_secondary_code(mut self, code: impl Into<String>) -> Self {
        if self.secondary_codes.len() < 2 {
            let code = code.into();
            let bounded = if !code.is_empty()
                && code.len() <= 64
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            {
                code
            } else {
                "ERROR_SECONDARY".to_owned()
            };
            if bounded != self.code && !self.secondary_codes.contains(&bounded) {
                self.secondary_codes.push(bounded);
            }
        }
        self
    }
    pub fn with_secondary_error(mut self, error: Self) -> Self {
        self = self.with_secondary_code(error.code);
        for code in error.secondary_codes {
            self = self.with_secondary_code(code);
        }
        self
    }
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new("INVALID_PARAMS", message)
    }
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new("CONFLICT", message)
    }
}
impl From<swarm_contracts::error::Error> for Error {
    fn from(value: swarm_contracts::error::Error) -> Self {
        Self {
            code: value.code,
            message: value.message,
            rejection_class: value.rejection_class,
            native_http_failure: value.native_http_failure,
            secondary_codes: Vec::new(),
        }
    }
}
impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::new("IO_ERROR", value.to_string())
    }
}
impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        let code = match &value {
            rusqlite::Error::SqliteFailure(error, _)
                if matches!(
                    error.code,
                    rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                ) =>
            {
                "STORE_BUSY"
            }
            _ => "STORE_ERROR",
        };
        Self::new(code, value.to_string())
    }
}
impl From<swarm_store::Error> for Error {
    fn from(value: swarm_store::Error) -> Self {
        match value {
            swarm_store::Error::Sqlite(error) => error.into(),
            other => Self::new(other.code(), other.message()),
        }
    }
}
impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::invalid(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use rusqlite::{Connection, ErrorCode};

    #[test]
    fn real_sqlite_contention_is_retryable_but_constraint_errors_are_hard() {
        let directory =
            std::env::temp_dir().join(format!("swarm-store-contention-{}", crate::model::new_id()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("store.sqlite");
        let first = Connection::open(&path).unwrap();
        let second = Connection::open(&path).unwrap();
        second.busy_timeout(std::time::Duration::ZERO).unwrap();
        first.execute_batch("CREATE TABLE data(value TEXT UNIQUE); INSERT INTO data VALUES('busy'); BEGIN IMMEDIATE;").unwrap();
        let busy = second
            .execute("INSERT INTO data VALUES('other')", [])
            .unwrap_err();
        assert!(
            matches!(&busy, rusqlite::Error::SqliteFailure(error, _) if error.code == ErrorCode::DatabaseBusy)
        );
        assert_eq!(
            Error::from(swarm_store::Error::Sqlite(busy)).code,
            "STORE_BUSY"
        );
        first.execute_batch("ROLLBACK").unwrap();
        let constraint = second
            .execute("INSERT INTO data VALUES('busy')", [])
            .unwrap_err();
        assert_eq!(Error::from(constraint).code, "STORE_ERROR");
        let mut statement = first.prepare("SELECT value FROM data").unwrap();
        let mut rows = statement.query([]).unwrap();
        assert!(rows.next().unwrap().is_some());
        let locked = first.execute("DROP TABLE data", []).unwrap_err();
        assert!(
            matches!(&locked, rusqlite::Error::SqliteFailure(error, _) if error.code == ErrorCode::DatabaseLocked)
        );
        assert_eq!(Error::from(locked).code, "STORE_BUSY");
        drop(rows);
        drop(statement);
        drop(second);
        drop(first);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
