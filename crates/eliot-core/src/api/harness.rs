//! Read-only harness installation, native model catalogue and quota DTOs.
//!
//! `harness.list` reports per-harness installation facts without starting a
//! model turn and without inference. `harness.models` returns the harness's
//! own read-only catalogue (Codex/Muse native `model/list`, the OpenCode
//! provider/model list; Claude exposes aliases plus the CLI default, and
//! Antigravity has no catalogue), cached per native route/account with a
//! freshness timestamp and a nonsecret credential identity. `harness.quota`
//! reports provider-supported account quota readback with timestamp, scope
//! and an explicit supported/unsupported/unknown status; it performs no
//! inference and never polls per Agent.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// Maximum harness name length.
pub const MAX_HARNESS_NAME_BYTES: usize = 32;
/// Maximum models returned in one catalogue page.
pub const MAX_HARNESS_MODELS: usize = 1024;
/// Maximum model id length.
pub const MAX_HARNESS_MODEL_ID_BYTES: usize = 256;
/// Maximum profile name length on catalogue/quota reads.
pub const MAX_HARNESS_PROFILE_BYTES: usize = 256;
/// Maximum native route/account scope key length.
pub const MAX_HARNESS_SCOPE_BYTES: usize = 512;
/// Maximum credential identity length (a name, never a secret value).
pub const MAX_HARNESS_CREDENTIAL_BYTES: usize = 256;
/// Maximum quota note length.
pub const MAX_HARNESS_QUOTA_NOTE_BYTES: usize = 1024;
/// Freshness bound for cached catalogue/quota readback: five minutes.
pub const HARNESS_READ_TTL_MS: u64 = 300_000;
/// Maximum freshness bound the daemon may serve.
pub const MAX_HARNESS_READ_TTL_MS: u64 = 3_600_000;

/// Request for `harness.list`. Empty object: no parameters.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessListRequest {}

impl HarnessListRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }

    pub fn validate_response(&self, reply: &HarnessListResponse) -> Result<()> {
        if reply.harnesses.is_empty() || reply.harnesses.len() > 16 {
            return Err(Error::new(
                "invalid",
                "harness list response is empty or oversized",
            ));
        }
        for entry in &reply.harnesses {
            if entry.harness.trim().is_empty() || entry.harness.len() > MAX_HARNESS_NAME_BYTES {
                return Err(Error::new(
                    "invalid",
                    "harness list entry names an invalid harness",
                ));
            }
        }
        Ok(())
    }
}

/// One harness installation fact.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEntry {
    /// Harness code: codex|claude|muse|opencode|antigravity.
    pub harness: String,
    /// True when the installed native executable was found.
    pub installed: bool,
    /// Resolved absolute native path, or null (Codex resolves `codex` by
    /// name at spawn; absent means not found).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cmd: Option<String>,
    /// Bounded `<exe> --version` first line, or null when unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Machine-readable failure code when not installed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Human-readable searched-locations detail when not installed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response for `harness.list`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessListResponse {
    pub harnesses: Vec<HarnessEntry>,
}

/// Request for `harness.models`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessModelsRequest {
    /// Harness code: codex|claude|muse|opencode|antigravity.
    pub harness: String,
    /// Optional named profile selecting the native route/account. Omitted or
    /// null means ad-hoc daemon defaults for the harness. A supplied profile
    /// must exist and its frozen harness must match `harness`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

impl HarnessModelsRequest {
    pub fn validate(&self) -> Result<()> {
        validate_harness(&self.harness)?;
        validate_profile_opt(self.profile.as_deref())?;
        Ok(())
    }

    pub fn validate_response(&self, reply: &HarnessModelsResponse) -> Result<()> {
        if reply.harness.trim() != self.harness.trim() {
            return Err(Error::new(
                "invalid",
                "harness models response names another harness",
            ));
        }
        reply.validate()
    }
}

fn validate_harness(harness: &str) -> Result<()> {
    let trimmed = harness.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_HARNESS_NAME_BYTES {
        return Err(Error::new(
            "invalid",
            "harness must be a non-empty string of at most 32 characters",
        ));
    }
    if !matches!(
        trimmed,
        "codex" | "claude" | "muse" | "opencode" | "antigravity"
    ) {
        return Err(Error::new(
            "unsupported",
            format!("unsupported harness {harness}"),
        ));
    }
    Ok(())
}

fn validate_profile_opt(profile: Option<&str>) -> Result<()> {
    if let Some(profile) = profile {
        let trimmed = profile.trim();
        if trimmed.is_empty()
            || trimmed.len() > MAX_HARNESS_PROFILE_BYTES
            || trimmed.chars().any(char::is_control)
        {
            return Err(Error::new(
                "invalid",
                "profile must be a non-empty string of at most 256 characters with no controls",
            ));
        }
    }
    Ok(())
}

fn validate_scope(scope: &str) -> Result<()> {
    if scope.trim().is_empty()
        || scope.len() > MAX_HARNESS_SCOPE_BYTES
        || scope.chars().any(char::is_control)
    {
        return Err(Error::new(
            "invalid",
            "harness scope must be a non-empty string of at most 512 characters with no controls",
        ));
    }
    Ok(())
}

fn validate_credential(credential: &str) -> Result<()> {
    if credential.trim().is_empty()
        || credential.len() > MAX_HARNESS_CREDENTIAL_BYTES
        || credential.chars().any(char::is_control)
    {
        return Err(Error::new(
            "invalid",
            "credential identity must be a non-empty name of at most 256 characters with no controls",
        ));
    }
    Ok(())
}

fn validate_freshness(fetched_at_ms: i64, ttl_ms: u64) -> Result<()> {
    if fetched_at_ms < 0 || ttl_ms == 0 || ttl_ms > MAX_HARNESS_READ_TTL_MS {
        return Err(Error::new(
            "invalid",
            "catalogue freshness must carry a non-negative fetch time and a positive TTL of at most one hour",
        ));
    }
    Ok(())
}

/// One catalogue model.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessModelEntry {
    /// Native model id (`provider/model` for OpenCode).
    pub id: String,
    /// True for the harness native default entry.
    #[serde(default)]
    pub default: bool,
    /// Supported reasoning efforts, when the harness reports them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub efforts: Option<Vec<String>>,
    /// The harness default reasoning effort, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_effort: Option<String>,
}

/// Response for `harness.models`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessModelsResponse {
    pub harness: String,
    /// `native` (live catalogue), `aliases` (Claude, no catalogue), or
    /// `none` (no catalogue; details in `note`).
    pub catalogue: String,
    pub models: Vec<HarnessModelEntry>,
    /// Native default model id, when the harness names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// Human-readable provenance note (aliases, CLI default, unsupported).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Nonsecret native route/account scope the catalogue was read under
    /// (harness, profile label and route selectors; never a credential value).
    pub scope: String,
    /// Milliseconds since the Unix epoch when the native read completed.
    /// A cached entry keeps its original fetch time; callers compute age as
    /// `now_ms - fetched_at_ms` against `ttl_ms`.
    pub fetched_at_ms: i64,
    /// Freshness bound in milliseconds for this entry.
    pub ttl_ms: u64,
    /// Nonsecret credential identity the read ran under (an environment
    /// name, integration name or ambient marker; never a secret value).
    pub credential: String,
}

impl HarnessModelsResponse {
    pub fn validate(&self) -> Result<()> {
        if self.models.len() > MAX_HARNESS_MODELS {
            return Err(Error::new(
                "invalid",
                "harness model catalogue exceeds 1024 entries",
            ));
        }
        for model in &self.models {
            if model.id.trim().is_empty()
                || model.id.len() > MAX_HARNESS_MODEL_ID_BYTES
                || model.id.chars().any(char::is_control)
            {
                return Err(Error::new(
                    "invalid",
                    "harness model id must be non-empty of at most 256 characters",
                ));
            }
        }
        validate_scope(&self.scope)?;
        validate_credential(&self.credential)?;
        validate_freshness(self.fetched_at_ms, self.ttl_ms)?;
        Ok(())
    }
}

/// Request for `harness.quota`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessQuotaRequest {
    /// Harness code: codex|claude|muse|opencode|antigravity.
    pub harness: String,
    /// Optional named profile selecting the native route/account. Omitted or
    /// null means ad-hoc daemon defaults for the harness. A supplied profile
    /// must exist and its frozen harness must match `harness`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

impl HarnessQuotaRequest {
    pub fn validate(&self) -> Result<()> {
        validate_harness(&self.harness)?;
        validate_profile_opt(self.profile.as_deref())?;
        Ok(())
    }

    pub fn validate_response(&self, reply: &HarnessQuotaResponse) -> Result<()> {
        if reply.harness.trim() != self.harness.trim() {
            return Err(Error::new(
                "invalid",
                "harness quota response names another harness",
            ));
        }
        reply.validate()
    }
}

/// Quota readback status: `supported` (provider read-only values present),
/// `unsupported` (the installed harness exposes no read-only quota surface)
/// or `unknown` (support could not be determined without an inference probe
/// or per-Agent read, so none was attempted).
pub const HARNESS_QUOTA_SUPPORTED: &str = "supported";
/// See [`HARNESS_QUOTA_SUPPORTED`].
pub const HARNESS_QUOTA_UNSUPPORTED: &str = "unsupported";
/// See [`HARNESS_QUOTA_SUPPORTED`].
pub const HARNESS_QUOTA_UNKNOWN: &str = "unknown";

/// Response for `harness.quota`: provider-supported account quota readback.
/// Carries only read-only provider signals with timestamp, scope and status;
/// it starts no model turn, performs no inference and never polls per Agent.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessQuotaResponse {
    pub harness: String,
    /// Nonsecret native route/account scope the quota was read under.
    pub scope: String,
    /// Milliseconds since the Unix epoch when support was determined.
    /// A cached entry keeps its original time; callers compute age as
    /// `now_ms - fetched_at_ms` against `ttl_ms`.
    pub fetched_at_ms: i64,
    /// Freshness bound in milliseconds for this entry.
    pub ttl_ms: u64,
    /// Nonsecret credential identity the read ran under (a name, never a value).
    pub credential: String,
    /// One of `supported`, `unsupported` or `unknown`.
    pub status: String,
    /// Human-readable detail: which native surface was checked, or the
    /// unsupported reason. Never a credential value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl HarnessQuotaResponse {
    pub fn validate(&self) -> Result<()> {
        if !matches!(
            self.status.as_str(),
            HARNESS_QUOTA_SUPPORTED | HARNESS_QUOTA_UNSUPPORTED | HARNESS_QUOTA_UNKNOWN
        ) {
            return Err(Error::new(
                "invalid",
                "harness quota status must be supported, unsupported or unknown",
            ));
        }
        if let Some(note) = self.note.as_deref()
            && (note.len() > MAX_HARNESS_QUOTA_NOTE_BYTES || note.chars().any(char::is_control))
        {
            return Err(Error::new(
                "invalid",
                "harness quota note must be at most 1024 characters with no controls",
            ));
        }
        validate_scope(&self.scope)?;
        validate_credential(&self.credential)?;
        validate_freshness(self.fetched_at_ms, self.ttl_ms)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn models_response(harness: &str, models: Vec<HarnessModelEntry>) -> HarnessModelsResponse {
        HarnessModelsResponse {
            harness: harness.to_owned(),
            catalogue: "native".to_owned(),
            models,
            default: None,
            note: None,
            scope: "profile:adhoc harness:codex route:direct".to_owned(),
            fetched_at_ms: 1,
            ttl_ms: HARNESS_READ_TTL_MS,
            credential: "ambient".to_owned(),
        }
    }

    #[test]
    fn models_request_validates_harness() {
        HarnessModelsRequest {
            harness: "codex".to_owned(),
            profile: None,
        }
        .validate()
        .unwrap();
        HarnessModelsRequest {
            harness: "codex".to_owned(),
            profile: Some("codex-main".to_owned()),
        }
        .validate()
        .unwrap();
        let error = HarnessModelsRequest {
            harness: "fake".to_owned(),
            profile: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(error.code, "unsupported");
        let error = HarnessModelsRequest {
            harness: String::new(),
            profile: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(error.code, "invalid");
        for profile in [
            Some(String::new()),
            Some("   ".to_owned()),
            Some("a".repeat(MAX_HARNESS_PROFILE_BYTES + 1)),
            Some("bad\nprofile".to_owned()),
        ] {
            let error = HarnessModelsRequest {
                harness: "codex".to_owned(),
                profile,
            }
            .validate()
            .unwrap_err();
            assert_eq!(error.code, "invalid");
        }
    }

    #[test]
    fn models_response_rejects_oversize_catalogue() {
        models_response("codex", vec![]).validate().unwrap();
        let big = models_response(
            "codex",
            (0..(MAX_HARNESS_MODELS + 1))
                .map(|index| HarnessModelEntry {
                    id: format!("m{index}"),
                    default: false,
                    efforts: None,
                    default_effort: None,
                })
                .collect(),
        );
        assert_eq!(big.validate().unwrap_err().code, "invalid");
    }

    #[test]
    fn models_response_requires_scope_and_freshness() {
        let mut response = models_response("codex", vec![]);
        response.scope = String::new();
        assert_eq!(response.validate().unwrap_err().code, "invalid");
        let mut response = models_response("codex", vec![]);
        response.credential = "  ".to_owned();
        assert_eq!(response.validate().unwrap_err().code, "invalid");
        let mut response = models_response("codex", vec![]);
        response.fetched_at_ms = -1;
        assert_eq!(response.validate().unwrap_err().code, "invalid");
        let mut response = models_response("codex", vec![]);
        response.ttl_ms = 0;
        assert_eq!(response.validate().unwrap_err().code, "invalid");
        let mut response = models_response("codex", vec![]);
        response.ttl_ms = MAX_HARNESS_READ_TTL_MS + 1;
        assert_eq!(response.validate().unwrap_err().code, "invalid");
        let request = HarnessModelsRequest {
            harness: "codex".to_owned(),
            profile: None,
        };
        let mut other = models_response("muse", vec![]);
        other.scope = "profile:adhoc harness:muse".to_owned();
        assert_eq!(
            request.validate_response(&other).unwrap_err().code,
            "invalid"
        );
    }

    fn quota_response(status: &str) -> HarnessQuotaResponse {
        HarnessQuotaResponse {
            harness: "codex".to_owned(),
            scope: "profile:adhoc harness:codex route:direct".to_owned(),
            fetched_at_ms: 1,
            ttl_ms: HARNESS_READ_TTL_MS,
            credential: "ambient".to_owned(),
            status: status.to_owned(),
            note: None,
        }
    }

    #[test]
    fn quota_request_validates_harness_and_profile() {
        HarnessQuotaRequest {
            harness: "muse".to_owned(),
            profile: None,
        }
        .validate()
        .unwrap();
        let error = HarnessQuotaRequest {
            harness: "fake".to_owned(),
            profile: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(error.code, "unsupported");
        let error = HarnessQuotaRequest {
            harness: "codex".to_owned(),
            profile: Some(String::new()),
        }
        .validate()
        .unwrap_err();
        assert_eq!(error.code, "invalid");
    }

    #[test]
    fn quota_response_validates_closed_status() {
        quota_response(HARNESS_QUOTA_UNSUPPORTED)
            .validate()
            .unwrap();
        quota_response(HARNESS_QUOTA_UNKNOWN).validate().unwrap();
        quota_response(HARNESS_QUOTA_SUPPORTED).validate().unwrap();
        let error = quota_response("limited").validate().unwrap_err();
        assert_eq!(error.code, "invalid");
        let mut response = quota_response(HARNESS_QUOTA_UNSUPPORTED);
        response.scope = "x".repeat(MAX_HARNESS_SCOPE_BYTES + 1);
        assert_eq!(response.validate().unwrap_err().code, "invalid");
        let mut response = quota_response(HARNESS_QUOTA_UNSUPPORTED);
        response.note = Some("n".repeat(MAX_HARNESS_QUOTA_NOTE_BYTES + 1));
        assert_eq!(response.validate().unwrap_err().code, "invalid");
        let request = HarnessQuotaRequest {
            harness: "codex".to_owned(),
            profile: None,
        };
        let mut other = quota_response(HARNESS_QUOTA_UNSUPPORTED);
        other.harness = "muse".to_owned();
        assert_eq!(
            request.validate_response(&other).unwrap_err().code,
            "invalid"
        );
    }
}
