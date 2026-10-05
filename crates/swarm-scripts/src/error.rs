use std::{error::Error as StdError, fmt};

pub type Result<T> = std::result::Result<T, ScriptError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScriptError {
    code: &'static str,
}

impl ScriptError {
    pub const fn new(code: &'static str) -> Self {
        Self { code }
    }

    pub const fn code(self) -> &'static str {
        self.code
    }
}

impl fmt::Display for ScriptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code)
    }
}

impl StdError for ScriptError {}

impl From<serde_json::Error> for ScriptError {
    fn from(_: serde_json::Error) -> Self {
        Self::new("SCRIPT_PROTOCOL_INVALID")
    }
}

impl From<swarm_bus::ValidationError> for ScriptError {
    fn from(_: swarm_bus::ValidationError) -> Self {
        Self::new("INVALID_PARAMS")
    }
}
