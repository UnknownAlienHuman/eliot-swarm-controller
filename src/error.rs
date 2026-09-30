use serde::Serialize;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Serialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct Error {
    pub code: String,
    pub message: String,
}

impl Error {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self { code: code.into(), message: message.into() }
    }
    pub fn invalid(message: impl Into<String>) -> Self { Self::new("INVALID_PARAMS", message) }
    pub fn conflict(message: impl Into<String>) -> Self { Self::new("CONFLICT", message) }
}
impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self { Self::new("IO_ERROR", value.to_string()) }
}
impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self { Self::new("STORE_ERROR", value.to_string()) }
}
impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self { Self::invalid(value.to_string()) }
}
