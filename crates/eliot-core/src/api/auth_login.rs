//! Local Owner sign-in requests and interactive desktop readback.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthLoginRequest {
    pub harness: String,
    pub revision: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthLoginStatus {
    Queued,
    Open,
    Cooldown,
}

impl AuthLoginStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Open => "open",
            Self::Cooldown => "cooldown",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AuthLoginResponse {
    pub status: AuthLoginStatus,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthDesktopPollRequest {}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AuthDesktopPollResponse {
    pub pending: Vec<AuthDesktopWork>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AuthDesktopWork {
    pub harness: String,
    pub revision: String,
    pub probe: bool,
    pub can_claim: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthDesktopClaimRequest {
    pub harness: String,
    pub revision: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct AuthDesktopClaimResponse {
    pub attempt: Option<String>,
    pub start_by_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthConsoleIdentity {
    pub pid: u32,
    pub start_time: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AuthDesktopReportRequest {
    pub harness: String,
    pub revision: String,
    pub attempt: Option<String>,
    pub state: AuthDesktopState,
    pub console: Option<AuthConsoleIdentity>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AuthDesktopState {
    SignedIn,
    SignedOut,
    Prepared,
    Open,
    Closed,
    Failed,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct AuthDesktopReportResponse {
    pub accepted: bool,
}
