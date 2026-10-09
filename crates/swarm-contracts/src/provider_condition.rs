//! Typed provider conditions used by the Store's route-admission gate.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderConditionSequenceKind {
    /// Sequence supplied by a provider protocol and documented as ordered.
    Native,
    /// Local collector revision. It orders reads within its connection only;
    /// it does not claim a provider-native clock or cross-connection order.
    LocalCollectionRevision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderCondition {
    Available,
    RateLimited { retry_at_ms: Option<i64> },
    QuotaExhausted { reset_at_ms: Option<i64> },
    ModelGone,
    DataPolicyRequired,
    AuthRequired,
    Overloaded { retry_at_ms: Option<i64> },
    Unknown { native_class: String },
}

/// One typed source fact, bound to the exact route and native binding that
/// supplied it. `native_sequence` is accompanied by an explicit kind because
/// some producers only have a local read revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConditionFact {
    pub schema_version: u16,
    pub binding_id: String,
    pub binding_generation: i64,
    pub route_alias: String,
    pub native_scope_key: String,
    pub provider_id: Option<String>,
    pub account_id: Option<String>,
    pub model_id: Option<String>,
    /// Exact native usage bucket for a bucket-scoped condition. `None` is
    /// reserved for the account-level ordinary-usage permission fact.
    pub bucket_id: Option<String>,
    /// Native model slug carried by this bucket, when the provider supplied it.
    pub source_model_slug: Option<String>,
    pub condition: ProviderCondition,
    pub observed_at_ms: i64,
    pub sequence_kind: ProviderConditionSequenceKind,
    pub native_sequence: String,
    pub source_connection_id: Option<String>,
    /// Present for Codex's ordinary-usage permission; this is the permission
    /// field's own provenance, not the enclosing snapshot's event method.
    pub evidence_method: Option<String>,
    pub evidence_revision: Option<u64>,
    /// Opaque SHA-256 of the actual native account identity, when proved.
    pub auth_context_ref: Option<String>,
}

impl ProviderConditionFact {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1
            || !text(&self.binding_id, 256)
            || self.binding_generation <= 0
            || !text(&self.route_alias, 256)
            || !text(&self.native_scope_key, 512)
            || self.observed_at_ms < 0
            || !text(&self.native_sequence, 256)
            || self.provider_id.as_deref().is_some_and(|v| !text(v, 256))
            || self.account_id.as_deref().is_some_and(|v| !text(v, 256))
            || self.model_id.as_deref().is_some_and(|v| !text(v, 256))
            || self.bucket_id.as_deref().is_some_and(|v| !text(v, 256))
            || self
                .source_model_slug
                .as_deref()
                .is_some_and(|v| !text(v, 256))
            || (self.source_model_slug.is_some() && self.bucket_id.is_none())
            || self
                .source_connection_id
                .as_deref()
                .is_some_and(|v| !token(v, 256))
            || self
                .evidence_method
                .as_deref()
                .is_some_and(|v| !token(v, 128))
            || self.auth_context_ref.as_deref().is_some_and(|v| !sha256(v))
        {
            return Err("invalid provider condition identity or bounds");
        }
        if self.sequence_kind == ProviderConditionSequenceKind::LocalCollectionRevision {
            let Some(revision) = self.evidence_revision.filter(|value| *value > 0) else {
                return Err("local provider sequence has no collector revision");
            };
            if self.source_connection_id.is_none() || self.native_sequence != revision.to_string() {
                return Err("local provider sequence is not bound to its connection revision");
            }
        }
        if let Some(revision) = self.evidence_revision
            && revision == 0
        {
            return Err("provider evidence revision must be positive");
        }
        match &self.condition {
            ProviderCondition::Available => {
                if self.sequence_kind != ProviderConditionSequenceKind::LocalCollectionRevision
                    || self.evidence_method.as_deref() != Some("account/rateLimits/read")
                    || self.auth_context_ref.is_none()
                {
                    return Err("Available requires the exact authenticated Codex permission read");
                }
            }
            ProviderCondition::RateLimited { retry_at_ms }
            | ProviderCondition::Overloaded { retry_at_ms } => {
                if retry_at_ms.is_some_and(|value| value < self.observed_at_ms) {
                    return Err("provider retry time precedes its condition");
                }
            }
            ProviderCondition::QuotaExhausted { reset_at_ms } => {
                if reset_at_ms.is_some_and(|value| value < self.observed_at_ms) {
                    return Err("provider reset time precedes its condition");
                }
            }
            ProviderCondition::Unknown { native_class } => {
                if !machine_class(native_class) {
                    return Err("unknown provider class must be a bounded machine label");
                }
            }
            ProviderCondition::ModelGone
            | ProviderCondition::DataPolicyRequired
            | ProviderCondition::AuthRequired => {}
        }
        if self.bucket_id.is_none()
            && (self.source_model_slug.is_some()
                || self.sequence_kind != ProviderConditionSequenceKind::LocalCollectionRevision
                || self.evidence_method.as_deref() != Some("account/rateLimits/read")
                || self.source_connection_id.is_none()
                || self.auth_context_ref.is_none()
                || !(matches!(&self.condition, ProviderCondition::Available)
                    || matches!(
                        &self.condition,
                        ProviderCondition::Unknown { native_class }
                            if native_class == "ordinary_usage_not_allowed"
                    )))
        {
            return Err("account condition requires its exact authenticated permission read");
        }
        if self.bucket_id.is_some()
            && (self.sequence_kind != ProviderConditionSequenceKind::LocalCollectionRevision
                || !matches!(
                    self.evidence_method.as_deref(),
                    Some("account/rateLimits/read" | "account/rateLimits/updated")
                )
                || self.source_connection_id.is_none())
        {
            return Err("bucket condition requires its exact Codex collection evidence");
        }
        Ok(())
    }
}

fn text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

fn token(value: &str, max: usize) -> bool {
    text(value, max)
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':' | b'/')
        })
}

fn machine_class(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
        })
}

fn sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
