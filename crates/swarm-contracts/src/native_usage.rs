//! Bounded subscription evidence. Collection order is never native time order.
use crate::provider_condition::ProviderCondition;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageService {
    Codex,
    Muse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageFreshness {
    Current,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageCompleteness {
    Full,
    Partial,
    NotObserved,
    Unsupported,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageCollectionIssue {
    ReadUnavailable,
    ReadUnsupported,
    InvalidPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageEvidence {
    pub method: String,
    /// Local collector revision, used only to reject an overtaken initial read.
    pub revision: u64,
    pub native_observed_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedUsageValue<T> {
    pub value: T,
    /// Retained separately when a rolling patch omits this metadata.
    pub collected_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedUsagePermission {
    pub value: bool,
    pub collected_at_ms: i64,
    /// The authoritative read which supplied this account-level permission.
    /// A rolling window notification does not refresh this provenance.
    pub evidence: UsageEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedProviderCondition {
    pub condition: ProviderCondition,
    pub collected_at_ms: i64,
    pub evidence: UsageEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageWindow {
    pub collected_at_ms: i64,
    pub used_percent: i64,
    pub resets_at_ms: Option<i64>,
    pub window_duration_mins: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageCredits {
    /// Verbatim native balance; its currency/unit may be unavailable.
    pub balance: Option<String>,
    pub has_credits: bool,
    pub unlimited: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageIndividualLimit {
    pub limit: String,
    pub used: String,
    pub remaining_percent: i64,
    pub resets_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageBucket {
    pub id: String,
    pub name: Option<ObservedUsageValue<String>>,
    pub normal_model_slug: Option<ObservedUsageValue<String>>,
    pub plan_type: Option<ObservedUsageValue<String>>,
    pub primary: Option<UsageWindow>,
    pub secondary: Option<UsageWindow>,
    pub credits: Option<ObservedUsageValue<UsageCredits>>,
    pub individual_limit: Option<ObservedUsageValue<UsageIndividualLimit>>,
    /// Null/unavailable does not establish recovery.
    pub spend_control_reached: Option<bool>,
    pub rate_limit_reached_type: Option<ObservedUsageValue<String>>,
    #[serde(default)]
    pub provider_condition: Option<ObservedProviderCondition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeUsageSnapshot {
    pub schema_version: u32,
    pub service: UsageService,
    /// Random identity of this actual native connection, not an account ID.
    pub connection_id: String,
    /// Opaque digest of independently observed auth context, or unknown.
    pub auth_context_ref: Option<String>,
    #[serde(default)]
    pub ordinary_usage_allowed: Option<ObservedUsagePermission>,
    #[serde(default)]
    pub collection_issue: Option<UsageCollectionIssue>,
    pub collected_at_ms: i64,
    pub freshness: UsageFreshness,
    pub completeness: UsageCompleteness,
    pub evidence: UsageEvidence,
    pub buckets: Vec<UsageBucket>,
}

impl NativeUsageSnapshot {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1
            || !token(&self.connection_id)
            || self.collected_at_ms <= 0
            || self.evidence.revision == 0
            || self.buckets.len() > 32
            || self
                .auth_context_ref
                .as_ref()
                .is_some_and(|v| v.len() != 64 || !v.bytes().all(|b| b.is_ascii_hexdigit()))
            || self.evidence.native_observed_at_ms.is_some_and(|v| v < 0)
        {
            return Err("invalid subscription evidence identity");
        }
        let supported = match self.service {
            UsageService::Codex => matches!(
                self.evidence.method.as_str(),
                "account/rateLimits/read"
                    | "account/rateLimits/updated"
                    | "account/updated"
                    | "connection/closed"
            ),
            UsageService::Muse => matches!(
                self.evidence.method.as_str(),
                "usage/read" | "usage/changed" | "connection/closed"
            ),
        };
        if !supported || (self.completeness == UsageCompleteness::Full && self.buckets.is_empty()) {
            return Err("invalid subscription evidence source");
        }
        if let Some(permission) = &self.ordinary_usage_allowed
            && (self.service != UsageService::Codex
                || !valid_collection(permission.collected_at_ms, self.collected_at_ms)
                || permission.evidence.method != "account/rateLimits/read"
                || permission.evidence.revision == 0
                || permission.evidence.revision > self.evidence.revision
                || permission.evidence.native_observed_at_ms.is_some())
        {
            return Err("invalid ordinary usage permission provenance");
        }
        let mut ids = BTreeSet::new();
        for bucket in &self.buckets {
            if !text(&bucket.id) || !ids.insert(&bucket.id) {
                return Err("invalid or repeated subscription bucket");
            }
            if let Some(condition) = &bucket.provider_condition {
                if self.service != UsageService::Codex
                    || !valid_collection(condition.collected_at_ms, self.collected_at_ms)
                    || !matches!(
                        condition.evidence.method.as_str(),
                        "account/rateLimits/read" | "account/rateLimits/updated"
                    )
                    || condition.evidence.revision == 0
                    || condition.evidence.revision > self.evidence.revision
                    || condition.evidence.native_observed_at_ms.is_some()
                    || matches!(condition.condition, ProviderCondition::Available)
                {
                    return Err("invalid provider condition provenance");
                }
                match &condition.condition {
                    ProviderCondition::RateLimited { retry_at_ms }
                    | ProviderCondition::Overloaded { retry_at_ms }
                        if retry_at_ms.is_some_and(|time| time < condition.collected_at_ms) =>
                    {
                        return Err("invalid provider retry time");
                    }
                    ProviderCondition::QuotaExhausted { reset_at_ms }
                        if reset_at_ms.is_some_and(|time| time < condition.collected_at_ms) =>
                    {
                        return Err("invalid provider reset time");
                    }
                    ProviderCondition::Unknown { native_class }
                        if native_class.is_empty()
                            || native_class.len() > 128
                            || !native_class.bytes().all(|byte| {
                                byte.is_ascii_lowercase()
                                    || byte.is_ascii_digit()
                                    || matches!(byte, b'_' | b'-' | b'.')
                            }) =>
                    {
                        return Err("invalid provider machine class");
                    }
                    _ => {}
                }
            }
            for metadata in [
                &bucket.name,
                &bucket.normal_model_slug,
                &bucket.plan_type,
                &bucket.rate_limit_reached_type,
            ]
            .into_iter()
            .flatten()
            {
                if !text(&metadata.value)
                    || !valid_collection(metadata.collected_at_ms, self.collected_at_ms)
                {
                    return Err("invalid subscription metadata");
                }
            }
            for window in [&bucket.primary, &bucket.secondary].into_iter().flatten() {
                if !valid_collection(window.collected_at_ms, self.collected_at_ms)
                    || window.used_percent < 0
                    || window.resets_at_ms.is_some_and(|v| v < 0)
                    || window.window_duration_mins.is_some_and(|v| v <= 0)
                {
                    return Err("invalid subscription window");
                }
            }
            if let Some(credits) = &bucket.credits
                && (!valid_collection(credits.collected_at_ms, self.collected_at_ms)
                    || credits.value.balance.as_ref().is_some_and(|v| !text(v)))
            {
                return Err("invalid subscription credits");
            }
            if let Some(limit) = &bucket.individual_limit
                && (!valid_collection(limit.collected_at_ms, self.collected_at_ms)
                    || !text(&limit.value.limit)
                    || !text(&limit.value.used)
                    || limit.value.resets_at_ms < 0)
            {
                return Err("invalid subscription spend limit");
            }
        }
        Ok(())
    }
}

fn valid_collection(value: i64, latest: i64) -> bool {
    value > 0 && value <= latest
}

fn text(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

fn token(value: &str) -> bool {
    text(value)
        && value
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || matches!(v, b'.' | b'_' | b'-' | b':'))
}
