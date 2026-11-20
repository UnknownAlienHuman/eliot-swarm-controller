//! Read-only harness installation, native model catalogue and quota DTOs.
//!
//! `harness.list` reports per-harness installation facts without starting a
//! model turn and without inference. `harness.models` returns the harness's
//! own read-only catalogue (Codex/Muse native `model/list`, OpenCode
//! `opencode models` provider/model ids, Antigravity `agy models`; Claude
//! prefers its native `initialize` inventory and falls back to its documented
//! aliases and full model ids plus the CLI default, each fallback entry
//! carrying `source = "documented_alias"`, only when the native read is
//! unavailable), cached per native
//! route/account with a freshness
//! timestamp and a nonsecret credential identity. `harness.quota`
//! reports provider-supported account quota readback with timestamp, scope,
//! an explicit supported/unsupported/unknown status and provider-verbatim
//! `details` when supported (Codex `account/rateLimits/read` plus
//! `account/usage/read`; Muse `usage/read`); it performs no inference and
//! never polls per Agent.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// The native harness adapters the controller supports.
pub const SUPPORTED_HARNESSES: &[&str] = &["codex", "claude", "muse", "opencode", "antigravity"];

/// Whether `name` is one of the supported native harnesses.
pub fn is_supported_harness(name: &str) -> bool {
    SUPPORTED_HARNESSES.contains(&name)
}

/// Maximum harness name length.
pub const MAX_HARNESS_NAME_BYTES: usize = 32;
/// Maximum models returned in one catalogue page.
pub const MAX_HARNESS_MODELS: usize = 1024;
/// Maximum model id length.
pub const MAX_HARNESS_MODEL_ID_BYTES: usize = 256;
/// Maximum native model display name length in UTF-8 bytes.
pub const MAX_HARNESS_MODEL_DISPLAY_NAME_BYTES: usize = 512;
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

/// Environment markers that select an external Claude route. The daemon
/// refuses them from the frozen environment and the Claude quota reader
/// refuses them from native `settings.json`; both share this one list.
pub const CLAUDE_EXTERNAL_ROUTE_MARKERS: &[&str] = &[
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "CLAUDE_CODE_USE_ANTHROPIC_AWS",
    "CLAUDE_CODE_USE_ANTHROPIC_GOOGLE_CLOUD",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
    "CLAUDE_CODE_USE_MANTLE",
];
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
    /// (for OpenCode, the owned generation's live catalogue). Exclusive
    /// with `profile`; the Agent's harness must match `harness`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// Existing workspace for an OpenCode launch preview. The live Agent
    /// already supplies its own workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
}

impl HarnessModelsRequest {
    pub fn validate(&self) -> Result<()> {
        validate_harness(&self.harness)?;
        validate_profile_opt(self.profile.as_deref())?;
        if let Some(workspace) = self.workspace.as_deref()
            && (self.agent.is_some()
                || self.harness.trim() != "opencode"
                || workspace.is_empty()
                || workspace.len() > 4096
                || workspace.chars().any(char::is_control))
        {
            return Err(Error::new(
                "invalid",
                "workspace requires an OpenCode catalogue preview and a valid path",
            ));
        }
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
    if !is_supported_harness(trimmed) {
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
    /// Verbatim native presentation label, when reported and valid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
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
    /// documented_alias for the frozen Claude alias and id list, used only
    /// when the native initialize inventory is unavailable. Availability is
    /// never asserted from catalogue membership.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

impl HarnessModelEntry {
    /// Presentation label with the native model id as the default label.
    pub fn display_name(&self) -> &str {
        self.display_name.as_deref().unwrap_or(&self.id)
    }
}

/// Accept a native catalogue label as-is when it fits the display contract.
pub fn native_model_display_name(name: &str) -> Option<&str> {
    (name.len() <= MAX_HARNESS_MODEL_DISPLAY_NAME_BYTES
        && !name.trim().is_empty()
        && !name.chars().any(char::is_control))
    .then_some(name)
}

/// Response for `harness.models`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessModelsResponse {
    pub harness: String,
    /// `native` (live catalogue, including Claude `initialize`), `aliases`
    /// (Claude fallback, with `source = "documented_alias"`), `empty` (the
    /// native read listed no models), or `none` (no catalogue). For OpenCode
    /// `note` identifies the live session or the launch preview source.
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
    /// Codex carries model and effort per turn; Muse switches the live
    /// session model through `session/setModel` and carries effort per
    /// turn; Claude switches the live session model through the
    /// stream-json `set_model` control request between turns; OpenCode
    /// sets the session model and its `Model.Ref.variant` natively;
    /// Antigravity sets `model` and `thought_level` natively. Anything
    /// else has no verified mechanism. Claude effort applies only at
    /// launch (`--effort`), with no verified live switch.
    pub fn for_harness(harness: &str) -> Self {
        let refuse = |reason: &str| Some(reason.to_owned());
        match harness {
            "codex" | "antigravity" | "opencode" | "muse" => Self::default(),
            "claude" => Self {
                model_refusal: None,
                effort_refusal: refuse(
                    "Claude effort applies only at launch and cannot change mid-session (launch_only)",
                ),
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

/// Muse stable schema's closed reasoning effort vocabulary.
pub const MUSE_REASONING_EFFORT_TIERS: [&str; 8] = [
    "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
];

/// Whether a dispatched `configure` must switch the live native session
/// through the owned plugin with native proof before the Router stores
/// anything. Mirrors [`ConfigureSupport`]: OpenCode and Antigravity always
/// switch natively; Claude and Muse switch natively only for a model
/// change (Muse effort-only stays a Store-only per-turn overlay; Claude
/// effort has no live switch and is refused at admission). Codex and
/// harnesses without a verified mechanism never switch natively, even
/// when a model is named.
pub fn configure_uses_native_session(harness: &str, args: &serde_json::Value) -> bool {
    if matches!(harness, "opencode" | "antigravity") {
        return true;
    }
    if matches!(harness, "claude" | "muse") {
        return args
            .get("model")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|model| !model.trim().is_empty());
    }
    false
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
            if model
                .display_name
                .as_deref()
                .is_some_and(|name| native_model_display_name(name).is_none())
            {
                return Err(Error::new(
                    "invalid",
                    "harness model display name must be non-blank, control-free and at most 512 bytes",
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
/// Maximum models in one `models.catalog` response (a validation bound on a
/// single page). A listing spans pages, so it has no total bound here.
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
/// cursor of the same selection. Results are bounded and
/// paginated.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelsCatalogRequest {
    /// Case-insensitive substring over model id, display name, provider and
    /// harness (at most 128 bytes). Omitted or empty means no filtering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Continue after the last cursor returned for the same selection.
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
                "catalogue cursor must be non-empty and at most 512 bytes",
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
    /// Verbatim native display name, or the native model id when absent.
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
    /// Named profile preset, with the profile's frozen model and effort.
    /// Ad-hoc rows omit this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

impl ModelsCatalogEntry {
    pub fn cursor(&self) -> String {
        match self.profile.as_deref() {
            Some(profile) => format!("profile:{}:{profile}", self.harness),
            None => catalog_cursor(&self.harness, &self.id),
        }
    }

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
        if native_model_display_name(&self.display_name).is_none() {
            return Err(Error::new(
                "invalid",
                "catalogue display name must be non-blank, control-free and at most 512 bytes",
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
        if let Some(profile) = self.profile.as_deref() {
            validate_profile_opt(Some(profile))?;
        }
        Ok(())
    }
}

/// Response for `models.catalog`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelsCatalogResponse {
    /// Every installed harness with its version and status.
    pub harnesses: Vec<HarnessEntry>,
    /// The bounded page of unified models, sorted by harness, id and profile.
    pub models: Vec<ModelsCatalogEntry>,
    /// Cursor after the last model on this page; absent at the end.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// Milliseconds since the Unix epoch when the aggregate was completed.
    pub fetched_at_ms: i64,
    /// Freshness bound in milliseconds for the aggregate.
    pub ttl_ms: u64,
    /// Installed harnesses whose catalogue read failed while this aggregate
    /// was built (repeated on every page). Such an aggregate is partial: it
    /// is not cached, so the next call reads the failed harness again.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<ModelsCatalogFailure>,
}

/// Maximum `models.catalog` failure message length in bytes.
pub const MAX_CATALOG_FAILURE_MESSAGE_BYTES: usize = 512;

/// One installed harness whose `models.catalog` read failed: the typed
/// error of its `harness.models` read.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelsCatalogFailure {
    /// Harness code: codex|claude|muse|opencode|antigravity.
    pub harness: String,
    /// The read's error code (`busy`, `unavailable`, `harness_error`, ...).
    pub code: String,
    /// The read's error message, bounded to 512 bytes, without controls.
    pub message: String,
    /// Who the failure is attributed to.
    pub origin: crate::FailureOrigin,
}

impl ModelsCatalogFailure {
    /// The failure of one harness's read, with its message bounded.
    pub fn from_error(harness: &str, error: &Error) -> Self {
        let mut message: String = error
            .message
            .chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect();
        if message.len() > MAX_CATALOG_FAILURE_MESSAGE_BYTES {
            let mut end = MAX_CATALOG_FAILURE_MESSAGE_BYTES;
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
        }
        Self {
            harness: harness.to_owned(),
            code: error.code.chars().take(64).collect(),
            message,
            origin: error.origin_or_unknown(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_harness(&self.harness)?;
        if self.code.trim().is_empty() || self.code.len() > 64 {
            return Err(Error::new(
                "invalid",
                "catalogue failure code must be non-empty of at most 64 bytes",
            ));
        }
        if self.message.len() > MAX_CATALOG_FAILURE_MESSAGE_BYTES
            || self.message.chars().any(char::is_control)
        {
            return Err(Error::new(
                "invalid",
                "catalogue failure message must be at most 512 bytes with no controls",
            ));
        }
        Ok(())
    }
}

impl ModelsCatalogResponse {
    pub fn validate(&self) -> Result<()> {
        if self.harnesses.is_empty() || self.harnesses.len() > 16 {
            return Err(Error::new(
                "invalid",
                "catalogue harness list is empty or oversized",
            ));
        }
        if self.failures.len() > self.harnesses.len() {
            return Err(Error::new(
                "invalid",
                "catalogue failures exceed the harness list",
            ));
        }
        for failure in &self.failures {
            failure.validate()?;
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
        || entry
            .profile
            .as_deref()
            .is_some_and(|profile| profile.to_ascii_lowercase().contains(&query))
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
        // Upstream MSP SubscriptionUsageWindow/Weekly allow integer
        // usedPercent >= 0 including values > 100 (over-quota is valid).
        // Canonical observations retain the finite nonnegative measurement;
        // only UI remaining-progress visualization clamps to 0..100.
        if self.observed_at_ms < 0
            || self.windows.is_empty()
            || self.windows.len() > 32
            || self.windows.iter().any(|w| {
                w.label.is_empty()
                    || w.label.len() > 128
                    || w.label.chars().any(char::is_control)
                    || !w.used_percent.is_finite()
                    || w.used_percent < 0.0
                    || w.resets_at_ms.is_some_and(|at| at < 0)
                    || w.window_duration_mins.is_some_and(|m| m <= 0)
            })
        {
            return Err(Error::new("invalid", "quota observation is invalid"));
        }
        Ok(())
    }
}
