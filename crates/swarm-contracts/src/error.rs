use serde::Serialize;

pub type Result<T> = std::result::Result<T, Error>;

/// Closed, non-sensitive class decoded from a native RPC rejection envelope.
/// Native error messages and provider data stay outside this classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeRpcRejectionClass {
    InvalidInput,
    MethodNotFound,
    Unavailable,
    InvalidOutput,
    Internal,
    Unclassified,
}

impl NativeRpcRejectionClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::MethodNotFound => "method_not_found",
            Self::Unavailable => "unavailable",
            Self::InvalidOutput => "invalid_output",
            Self::Internal => "internal",
            Self::Unclassified => "unclassified",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "invalid_input" => Some(Self::InvalidInput),
            "method_not_found" => Some(Self::MethodNotFound),
            "unavailable" => Some(Self::Unavailable),
            "invalid_output" => Some(Self::InvalidOutput),
            "internal" => Some(Self::Internal),
            "unclassified" => Some(Self::Unclassified),
            _ => None,
        }
    }
}

/// Closed, non-sensitive class decoded from a bounded OpenCode V2 HTTP error
/// envelope. Native messages and resource identifiers are never retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeHttpFailureKind {
    InvalidRequest,
    Unauthorized,
    Conflict,
    SessionNotFound,
    MessageNotFound,
    InternalServerError,
    Unclassified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct NativeHttpFailure {
    pub status: u16,
    pub kind: NativeHttpFailureKind,
}

#[derive(Debug, Clone, Serialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct Error {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rejection_class: Option<NativeRpcRejectionClass>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_http_failure: Option<NativeHttpFailure>,
}

impl Error {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            rejection_class: None,
            native_http_failure: None,
        }
    }

    pub fn with_rejection_class(mut self, rejection_class: NativeRpcRejectionClass) -> Self {
        self.rejection_class = Some(rejection_class);
        self
    }

    pub fn with_native_http_failure(mut self, failure: NativeHttpFailure) -> Self {
        self.native_http_failure = Some(failure);
        self
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new("INVALID_PARAMS", message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new("CONFLICT", message)
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::new("IO_ERROR", value.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::invalid(value.to_string())
    }
}
