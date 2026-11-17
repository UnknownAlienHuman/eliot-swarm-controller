//! Read-only harness installation, native model catalogue and quota DTOs.
//!
//! `harness.list` reports per-harness installation facts without starting a
//! model turn and without inference. `harness.models` returns the harness's
//! own read-only catalogue (Codex/Muse native `model/list`, OpenCode
//! `opencode models` provider/model ids, Antigravity `agy models`; Claude
//! exposes its documented aliases and full model ids plus the CLI default,
//! each entry carrying `source = "documented_alias"`), cached per native
//! route/account with a freshness
//! timestamp and a nonsecret credential identity. `harness.quota`
//! reports provider-supported account quota readback with timestamp, scope,
//! an explicit supported/unsupported/unknown status and provider-verbatim
//! `details` when supported (Codex `account/rateLimits/read` plus
//! `account/usage/read`; Muse `usage/read`); it performs no inference and
//! never polls per Agent.

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
/// Maximum catalogue entry source marker length.
pub const MAX_HARNESS_SOURCE_BYTES: usize = 64;
/// Maximum quota note length.
pub const MAX_HARNESS_QUOTA_NOTE_BYTES: usize = 1024;
/// Maximum encoded bytes for structured quota details (provider-verbatim
/// quota signals, never secrets or personal data).
pub const MAX_HARNESS_QUOTA_DETAILS_BYTES: usize = 8192;
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
    /// Every discovered native install, newest first (currently Claude
    /// only: PATH, standard locations and desktop-app bundles, each with
    /// its own `--version`). Absent for other harnesses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidates: Option<Vec<HarnessCandidate>>,
}

/// One discovered native install for a harness.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessCandidate {
    /// Absolute native executable path.
    pub cmd: String,
    /// Bounded `<exe> --version` first line, or null when unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
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
    /// Optional Agent whose frozen launch snapshot selects the route/account
    /// (the catalogue that Agent's `configure` draws from). Exclusive with
    /// `profile`; the Agent's harness must match `harness`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
}

impl HarnessModelsRequest {
    pub fn validate(&self) -> Result<()> {
        validate_harness(&self.harness)?;
        validate_profile_opt(self.profile.as_deref())?;
        if let Some(agent) = self.agent.as_deref() {
            if self.profile.is_some() {
                return Err(Error::new(
                    "invalid",
                    "name a profile or an Agent route, not both",
                ));
            }
            crate::parse_agent_id(agent)?;
        }
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

fn validate_source(source: Option<&str>) -> Result<()> {
    if let Some(source) = source {
        let trimmed = source.trim();
        if trimmed.is_empty()
            || trimmed.len() > MAX_HARNESS_SOURCE_BYTES
            || trimmed.chars().any(char::is_control)
        {
            return Err(Error::new(
                "invalid",
                "catalogue entry source must be a non-empty string of at most 64 characters with no controls",
            ));
        }
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
    /// Provenance of this entry: absent for a live native read, or
    /// `documented_alias` for the frozen Claude alias/id list (Claude Code
    /// exposes no read-only model inventory; availability is never
    /// asserted from catalogue membership).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// Response for `harness.models`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessModelsResponse {
    pub harness: String,
    /// `native` (live catalogue), `aliases` (Claude documented aliases and
    /// full model ids, each entry carrying `source = "documented_alias"`),
    /// `empty` (the native read ran but listed no models; details in
    /// `note`), or `none` (no catalogue; details in `note`).
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
    /// Which mid-session `configure` fields this harness applies.
    #[serde(default)]
    pub configure: ConfigureSupport,
}

/// Mid-session `configure` support of one harness: `None` when the field
/// has a verified native mechanism, otherwise the exact refusal that
/// `configure` admission returns (`unsupported`) for it. Admission and the
/// `harness.models` readback share this one table.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigureSupport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_refusal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort_refusal: Option<String>,
}

impl ConfigureSupport {
    /// Codex carries model and effort per turn; Muse carries effort per
    /// turn but a model change needs a new native session; OpenCode sets
    /// the session model and its `Model.Ref.variant` natively;
    /// Antigravity sets `model` and `thought_level` natively. Anything
    /// else has no verified mechanism.
    pub fn for_harness(harness: &str) -> Self {
        let refuse = |reason: &str| Some(reason.to_owned());
        match harness {
            "codex" | "antigravity" | "opencode" => Self::default(),
            "muse" => Self {
                model_refusal: refuse(
                    "Muse model changes require a new native session (restart_required)",
                ),
                effort_refusal: None,
            },
            other => {
                let reason = format!("configure is not supported for the {other} harness");
                Self {
                    model_refusal: Some(reason.clone()),
                    effort_refusal: Some(reason),
                }
            }
        }
    }
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
            validate_source(model.source.as_deref())?;
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

/// Quota readback status: `supported` (known source; details state is ready or error),
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
/// A `supported` response carries projected provider quota signals or a sanitized error in
/// `details` (Codex rate-limit windows with used percent, reset time,
/// credits and plan; Muse subscription usage windows with used percent,
/// reset time and tier); never secrets or personal data.
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
    /// Provider-verbatim quota signals for `supported` reads (an object:
    /// Codex `rateLimits` windows or Muse `usage` windows). Omitted unless
    /// `status` is `supported`; never carries secrets, emails, labels or
    /// other personal data.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
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
        if let Some(details) = self.details.as_ref() {
            if !details.is_object() {
                return Err(Error::new(
                    "invalid",
                    "harness quota details must be an object",
                ));
            }
            let encoded = serde_json::to_vec(details)
                .map_err(|_| Error::new("invalid", "harness quota details could not be encoded"))?;
            if encoded.len() > MAX_HARNESS_QUOTA_DETAILS_BYTES {
                return Err(Error::new(
                    "invalid",
                    "harness quota details exceeds 8192 encoded bytes",
                ));
            }
        }
        if self.status.as_str() != HARNESS_QUOTA_SUPPORTED && self.details.is_some() {
            return Err(Error::new(
                "invalid",
                "harness quota details requires supported status",
            ));
        }
        validate_scope(&self.scope)?;
        validate_credential(&self.credential)?;
        validate_freshness(self.fetched_at_ms, self.ttl_ms)?;
        Ok(())
    }
}

/// Maximum `models.catalog` query length in bytes.
pub const MAX_CATALOG_QUERY_BYTES: usize = 128;
/// Maximum `models.catalog` page size.
pub const MAX_CATALOG_LIMIT: usize = 100;
/// Default `models.catalog` page size.
pub const DEFAULT_CATALOG_LIMIT: usize = 50;
/// Maximum models returned in one `models.catalog` page is the page limit;
/// the aggregated catalogue behind the page is bounded by the per-harness cap.
pub const MAX_CATALOG_MODELS: usize = 1024;

/// Availability of one catalogue model: `available` (provider quota supports
/// it and no hold covers it), `held` (a provider hold covers its harness
/// scope) or `unknown` (no hold/quota signal is known).
pub const CATALOG_AVAILABLE: &str = "available";
/// See [`CATALOG_AVAILABLE`].
pub const CATALOG_HELD: &str = "held";
/// See [`CATALOG_AVAILABLE`].
pub const CATALOG_UNKNOWN: &str = "unknown";

/// Request for `models.catalog`: one owner-scoped read-only search over every
/// installed harness catalogue. `query` filters case-insensitively over model
/// id, display name, provider and harness. `after` is the last returned
/// `harness:id` cursor of the same selection. Results are bounded and
/// paginated.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelsCatalogRequest {
    /// Case-insensitive substring over model id, display name, provider and
    /// harness (at most 128 bytes). Omitted or empty means no filtering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Continue after the last `harness:id` cursor of the same selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    /// Page size, 1..=100; default 50.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

impl ModelsCatalogRequest {
    pub fn effective_limit(&self) -> usize {
        self.limit.unwrap_or(DEFAULT_CATALOG_LIMIT)
    }

    pub fn validate(&self) -> Result<()> {
        if self
            .query
            .as_deref()
            .is_some_and(|query| query.len() > MAX_CATALOG_QUERY_BYTES)
        {
            return Err(Error::new(
                "invalid",
                "catalogue query must be at most 128 bytes",
            ));
        }
        if self.after.as_deref().is_some_and(|after| {
            after.trim().is_empty() || after.len() > 512 || after.chars().any(char::is_control)
        }) {
            return Err(Error::new(
                "invalid",
                "catalogue cursor must be a non-empty harness:id of at most 512 bytes",
            ));
        }
        if !(1..=MAX_CATALOG_LIMIT).contains(&self.effective_limit()) {
            return Err(Error::new(
                "invalid",
                "catalogue limit must be from 1 through 100",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &ModelsCatalogResponse) -> Result<()> {
        if reply.models.len() > self.effective_limit() {
            return Err(Error::new("invalid", "catalogue page exceeds its limit"));
        }
        reply.validate()
    }
}

/// One unified catalogue model.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelsCatalogEntry {
    /// Harness code: codex|claude|muse|opencode|antigravity.
    pub harness: String,
    /// Native model id verbatim (`provider/model` for OpenCode).
    pub id: String,
    /// Display name (native id verbatim; OpenCode shows the full
    /// `provider/model` id).
    pub display_name: String,
    /// Provider (`openai` for Codex, `anthropic` for Claude, `meta` for Muse,
    /// the `provider/` prefix for OpenCode, the leading family for
    /// Antigravity, otherwise the harness code).
    pub provider: String,
    /// Supported reasoning efforts or variants, when the harness reports them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub efforts: Option<Vec<String>>,
    /// The harness default reasoning effort, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_effort: Option<String>,
    /// True for the harness native default entry.
    #[serde(default)]
    pub default: bool,
    /// One of `available`, `held` or `unknown`.
    pub availability: String,
    /// Provenance of this entry, copied from the harness catalogue:
    /// absent for a live native read, or `documented_alias` for the
    /// frozen Claude alias/id list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

impl ModelsCatalogEntry {
    pub fn validate(&self) -> Result<()> {
        validate_harness(&self.harness)?;
        if self.id.trim().is_empty()
            || self.id.len() > MAX_HARNESS_MODEL_ID_BYTES
            || self.id.chars().any(char::is_control)
        {
            return Err(Error::new(
                "invalid",
                "catalogue model id must be non-empty of at most 256 characters",
            ));
        }
        if self.display_name.trim().is_empty() || self.display_name.len() > 512 {
            return Err(Error::new(
                "invalid",
                "catalogue display name must be non-empty of at most 512 characters",
            ));
        }
        if self.provider.trim().is_empty() || self.provider.len() > 128 {
            return Err(Error::new(
                "invalid",
                "catalogue provider must be non-empty of at most 128 characters",
            ));
        }
        if !matches!(
            self.availability.as_str(),
            CATALOG_AVAILABLE | CATALOG_HELD | CATALOG_UNKNOWN
        ) {
            return Err(Error::new(
                "invalid",
                "catalogue availability must be available, held or unknown",
            ));
        }
        validate_source(self.source.as_deref())?;
        Ok(())
    }
}

/// Response for `models.catalog`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelsCatalogResponse {
    /// Every installed harness with its version and status.
    pub harnesses: Vec<HarnessEntry>,
    /// The bounded page of unified models, sorted by harness then id.
    pub models: Vec<ModelsCatalogEntry>,
    /// Last `harness:id` cursor of this page; absent at the end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// Milliseconds since the Unix epoch when the aggregate was completed.
    pub fetched_at_ms: i64,
    /// Freshness bound in milliseconds for the aggregate.
    pub ttl_ms: u64,
}

impl ModelsCatalogResponse {
    pub fn validate(&self) -> Result<()> {
        if self.harnesses.is_empty() || self.harnesses.len() > 16 {
            return Err(Error::new(
                "invalid",
                "catalogue harness list is empty or oversized",
            ));
        }
        if self.models.len() > MAX_CATALOG_MODELS {
            return Err(Error::new(
                "invalid",
                "catalogue model list exceeds 1024 entries",
            ));
        }
        for model in &self.models {
            model.validate()?;
        }
        validate_freshness(self.fetched_at_ms, self.ttl_ms)?;
        Ok(())
    }
}

/// Provider for one catalogue id: OpenCode and Antigravity models carry a
/// provider prefix or group; Codex is openai, Claude is anthropic, Muse is
/// meta. Documented in `docs/spec/configuration.md` (access mapping).
pub fn catalog_provider(harness: &str, id: &str) -> String {
    if let Some((provider, _)) = id.split_once('/')
        && !provider.trim().is_empty()
    {
        return provider.to_owned();
    }
    match harness {
        "codex" => "openai".to_owned(),
        "claude" => "anthropic".to_owned(),
        "muse" => "meta".to_owned(),
        "antigravity" => id
            .split(['-', '_'])
            .next()
            .filter(|family| !family.trim().is_empty())
            .unwrap_or("antigravity")
            .to_owned(),
        "opencode" => id
            .split_once('/')
            .map(|(provider, _)| provider.to_owned())
            .unwrap_or_else(|| "opencode".to_owned()),
        _ => harness.to_owned(),
    }
}

/// Display name for one catalogue id: the native id verbatim.
pub fn catalog_display_name(id: &str) -> String {
    id.to_owned()
}

/// Cursor for one catalogue entry: `harness:id`.
pub fn catalog_cursor(harness: &str, id: &str) -> String {
    format!("{harness}:{id}")
}

/// Case-insensitive substring match over model id, display name, provider
/// and harness. An empty query matches everything.
pub fn catalog_matches(entry: &ModelsCatalogEntry, query: &str) -> bool {
    let query = query.trim();
    if query.is_empty() {
        return true;
    }
    let query = query.to_ascii_lowercase();
    [
        entry.id.as_str(),
        entry.display_name.as_str(),
        entry.provider.as_str(),
        entry.harness.as_str(),
    ]
    .iter()
    .any(|field| field.to_ascii_lowercase().contains(&query))
}

/// Provider quota observed in a native stream, never inferred from token expenditure.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuotaObservation {
    pub observed_at_ms: i64,
    pub windows: Vec<QuotaWindow>,
}
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct QuotaWindow {
    pub label: String,
    pub used_percent: f64,
    pub resets_at_ms: Option<i64>,
    pub window_duration_mins: Option<i64>,
}
impl QuotaObservation {
    pub fn validate(&self) -> Result<()> {
        if self.observed_at_ms < 0
            || self.windows.is_empty()
            || self.windows.len() > 32
            || self.windows.iter().any(|w| {
                w.label.is_empty()
                    || w.label.len() > 128
                    || w.label.chars().any(char::is_control)
                    || !w.used_percent.is_finite()
                    || !(0.0..=100.0).contains(&w.used_percent)
                    || w.resets_at_ms.is_some_and(|at| at < 0)
                    || w.window_duration_mins.is_some_and(|m| m <= 0)
            })
        {
            return Err(Error::new("invalid", "quota observation is invalid"));
        }
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
            configure: ConfigureSupport::for_harness(harness),
        }
    }

    #[test]
    fn models_request_validates_harness() {
        HarnessModelsRequest {
            harness: "codex".to_owned(),
            profile: None,
            agent: None,
        }
        .validate()
        .unwrap();
        HarnessModelsRequest {
            harness: "codex".to_owned(),
            profile: Some("codex-main".to_owned()),
            agent: None,
        }
        .validate()
        .unwrap();
        let error = HarnessModelsRequest {
            harness: "fake".to_owned(),
            profile: None,
            agent: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(error.code, "unsupported");
        let error = HarnessModelsRequest {
            harness: String::new(),
            profile: None,
            agent: None,
        }
        .validate()
        .unwrap_err();
        assert_eq!(error.code, "invalid");
    }

    #[test]
    fn models_request_takes_a_profile_or_an_agent_route() {
        let request = |profile: Option<&str>, agent: Option<&str>| HarnessModelsRequest {
            harness: "codex".to_owned(),
            profile: profile.map(str::to_owned),
            agent: agent.map(str::to_owned),
        };
        request(None, Some("a7")).validate().unwrap();
        assert_eq!(
            request(Some("p"), Some("a7")).validate().unwrap_err().code,
            "invalid"
        );
        assert!(request(None, Some("seven")).validate().is_err());
    }

    #[test]
    fn configure_support_names_each_refusal() {
        assert_eq!(
            ConfigureSupport::for_harness("codex"),
            ConfigureSupport::default()
        );
        assert_eq!(
            ConfigureSupport::for_harness("antigravity"),
            ConfigureSupport::default()
        );
        let muse = ConfigureSupport::for_harness("muse");
        assert!(muse.model_refusal.unwrap().contains("restart_required"));
        assert!(muse.effort_refusal.is_none());
        let opencode = ConfigureSupport::for_harness("opencode");
        assert!(opencode.model_refusal.is_none());
        assert!(opencode.effort_refusal.is_none());
        let claude = ConfigureSupport::for_harness("claude");
        assert!(claude.model_refusal.is_some() && claude.effort_refusal.is_some());
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
                    source: None,
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
            agent: None,
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
            details: None,
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

    #[test]
    fn quota_response_validates_details_object() {
        let mut supported = quota_response(HARNESS_QUOTA_SUPPORTED);
        supported.details = Some(serde_json::json!({
            "rateLimits": {"primary": {"usedPercent": 12, "resetsAt": 1_700_000_000}},
        }));
        supported.validate().unwrap();
        let mut non_object = quota_response(HARNESS_QUOTA_SUPPORTED);
        non_object.details = Some(serde_json::json!([1, 2, 3]));
        assert_eq!(non_object.validate().unwrap_err().code, "invalid");
        let mut unsupported_with_details = quota_response(HARNESS_QUOTA_UNSUPPORTED);
        unsupported_with_details.details = Some(serde_json::json!({"usage": {}}));
        assert_eq!(
            unsupported_with_details.validate().unwrap_err().code,
            "invalid"
        );
        let mut oversize = quota_response(HARNESS_QUOTA_SUPPORTED);
        oversize.details = Some(serde_json::json!({
            "blob": "x".repeat(MAX_HARNESS_QUOTA_DETAILS_BYTES)
        }));
        assert_eq!(oversize.validate().unwrap_err().code, "invalid");
    }

    fn catalog_entry(harness: &str, id: &str) -> ModelsCatalogEntry {
        ModelsCatalogEntry {
            harness: harness.to_owned(),
            id: id.to_owned(),
            display_name: catalog_display_name(id),
            provider: catalog_provider(harness, id),
            efforts: None,
            default_effort: None,
            default: false,
            availability: CATALOG_UNKNOWN.to_owned(),
            source: None,
        }
    }

    #[test]
    fn catalogue_entry_source_marks_documented_aliases() {
        let mut entry = catalog_entry("claude", "haiku");
        entry.source = Some("documented_alias".to_owned());
        entry.validate().unwrap();
        let oversize = "x".repeat(MAX_HARNESS_SOURCE_BYTES + 1);
        for source in ["".to_owned(), "  ".to_owned(), oversize] {
            let mut bad = catalog_entry("claude", "haiku");
            bad.source = Some(source);
            assert_eq!(bad.validate().unwrap_err().code, "invalid");
        }
        let mut control = catalog_entry("claude", "haiku");
        control.source = Some(
            "a
b"
            .to_owned(),
        );
        assert_eq!(control.validate().unwrap_err().code, "invalid");
        let mut response = models_response(
            "claude",
            vec![HarnessModelEntry {
                id: "haiku".to_owned(),
                default: false,
                efforts: None,
                default_effort: None,
                source: Some("documented_alias".to_owned()),
            }],
        );
        response.validate().unwrap();
        response.models[0].source = Some(String::new());
        assert_eq!(response.validate().unwrap_err().code, "invalid");
    }

    #[test]
    fn catalog_provider_names_each_harness() {
        assert_eq!(catalog_provider("codex", "gpt-6-luna"), "openai");
        assert_eq!(catalog_provider("claude", "claude-haiku-5-5"), "anthropic");
        assert_eq!(catalog_provider("muse", "muse-spark-1.3"), "meta");
        assert_eq!(
            catalog_provider("opencode", "opencode/deepseek-v4.1-flash-free"),
            "opencode"
        );
        assert_eq!(catalog_provider("opencode", "deepseek/v4-free"), "deepseek");
        assert_eq!(
            catalog_provider("antigravity", "gemini-4-hydrogen-low"),
            "gemini"
        );
    }

    #[test]
    fn catalog_query_filters_case_insensitively_over_all_fields() {
        let luna = catalog_entry("codex", "gpt-6-luna");
        assert!(catalog_matches(&luna, ""));
        assert!(catalog_matches(&luna, "luna"));
        assert!(catalog_matches(&luna, "LUNA"));
        assert!(catalog_matches(&luna, "GPT-6"));
        assert!(catalog_matches(&luna, "openai"));
        assert!(catalog_matches(&luna, "OPENAI"));
        assert!(catalog_matches(&luna, "codex"));
        assert!(catalog_matches(&luna, "CoDeX"));
        assert!(!catalog_matches(&luna, "anthropic"));
        let opencode = catalog_entry("opencode", "opencode/deepseek-v4.1-flash-free");
        assert!(catalog_matches(&opencode, "deepseek"));
        assert!(catalog_matches(&opencode, "opencode"));
        assert!(!catalog_matches(&opencode, "luna"));
    }

    #[test]
    fn catalog_request_validates_bounds() {
        ModelsCatalogRequest {
            query: None,
            after: None,
            limit: None,
        }
        .validate()
        .unwrap();
        assert_eq!(
            ModelsCatalogRequest {
                query: Some("x".repeat(MAX_CATALOG_QUERY_BYTES + 1)),
                after: None,
                limit: None,
            }
            .validate()
            .unwrap_err()
            .code,
            "invalid"
        );
        for limit in [0, MAX_CATALOG_LIMIT + 1] {
            assert_eq!(
                ModelsCatalogRequest {
                    query: None,
                    after: None,
                    limit: Some(limit),
                }
                .validate()
                .unwrap_err()
                .code,
                "invalid"
            );
        }
        let page = ModelsCatalogResponse {
            harnesses: vec![HarnessEntry {
                harness: "codex".to_owned(),
                installed: true,
                cmd: None,
                version: Some("1.0".to_owned()),
                code: None,
                error: None,
                candidates: None,
            }],
            models: vec![catalog_entry("codex", "gpt-6-luna")],
            next: None,
            fetched_at_ms: 1,
            ttl_ms: HARNESS_READ_TTL_MS,
        };
        ModelsCatalogRequest {
            query: None,
            after: None,
            limit: Some(1),
        }
        .validate_response(&page)
        .unwrap();
        assert_eq!(
            ModelsCatalogRequest {
                query: None,
                after: None,
                limit: Some(1),
            }
            .validate_response(&ModelsCatalogResponse {
                models: vec![catalog_entry("codex", "a"), catalog_entry("codex", "b"),],
                ..page
            })
            .unwrap_err()
            .code,
            "invalid"
        );
    }
}
