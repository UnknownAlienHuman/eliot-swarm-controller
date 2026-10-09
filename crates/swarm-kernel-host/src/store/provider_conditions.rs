//! Exact typed provider-condition retention and the shared route decision.
use super::capacity;
use crate::{
    config::Route,
    error::{Error, Result},
    model,
};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use swarm_contracts::{
    native_usage::{NativeUsageSnapshot, UsageCompleteness, UsageFreshness, UsageService},
    provider_condition::{ProviderCondition, ProviderConditionFact, ProviderConditionSequenceKind},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResourceScope {
    pub scope_key: String,
    pub route_alias: String,
    pub runtime_id: String,
    pub service_id: Option<String>,
    pub provider_id: Option<String>,
    pub account_id: Option<String>,
    pub model_id: Option<String>,
    pub binding_id: Option<String>,
    pub binding_generation: Option<i64>,
    pub native_scope_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConditionWrite {
    Updated,
    Unchanged,
    InvalidatedAvailable,
    NoAuthoritativeFact,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub(super) enum RouteAdmissionDecision {
    Admit,
    Hold { code: String, until_ms: Option<i64> },
    Unavailable { code: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(super) enum ConditionProjection {
    ConditionUnobserved,
    Observed {
        condition: ProviderCondition,
        observed_at_ms: i64,
        source_observation_id: i64,
        bucket_id: Option<String>,
        source_model_slug: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(super) enum RootLimitProjection {
    LimitNotConfigured,
    WithinLimit {
        max_concurrent_roots: u16,
        current_claims: u32,
    },
    LimitReached {
        max_concurrent_roots: u16,
        current_claims: u32,
    },
    CapacityUnknown {
        max_concurrent_roots: u16,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RouteAdmissionProjection {
    pub decision: RouteAdmissionDecision,
    pub condition: ConditionProjection,
    pub root_limit: RootLimitProjection,
}

#[derive(Debug, Clone)]
struct RetainedCondition {
    scope: ResourceScope,
    fact: ProviderConditionFact,
    source_module_sequence: i64,
    source_epoch: String,
    source_observation_id: i64,
    details_digest: String,
}

struct StoredCondition {
    scope_key: String,
    bucket_id: String,
    source_model_slug: Option<String>,
    route_alias: String,
    runtime_id: String,
    service_id: Option<String>,
    binding_id: String,
    binding_generation: i64,
    native_scope_key: String,
    provider_id: Option<String>,
    account_id: Option<String>,
    model_id: Option<String>,
    condition_kind: String,
    retry_at_ms: Option<i64>,
    reset_at_ms: Option<i64>,
    unknown_native_class: Option<String>,
    observed_at_ms: i64,
    sequence_kind: String,
    native_sequence: String,
    source_connection_id: Option<String>,
    evidence_method: Option<String>,
    evidence_revision: Option<i64>,
    auth_context_ref: Option<String>,
    source_module_sequence: i64,
    source_epoch: String,
    source_observation_id: i64,
    details_digest: String,
}

impl StoredCondition {
    fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            scope_key: row.get(0)?,
            bucket_id: row.get(1)?,
            source_model_slug: row.get(2)?,
            route_alias: row.get(3)?,
            runtime_id: row.get(4)?,
            service_id: row.get(5)?,
            binding_id: row.get(6)?,
            binding_generation: row.get(7)?,
            native_scope_key: row.get(8)?,
            provider_id: row.get(9)?,
            account_id: row.get(10)?,
            model_id: row.get(11)?,
            condition_kind: row.get(12)?,
            retry_at_ms: row.get(13)?,
            reset_at_ms: row.get(14)?,
            unknown_native_class: row.get(15)?,
            observed_at_ms: row.get(16)?,
            sequence_kind: row.get(17)?,
            native_sequence: row.get(18)?,
            source_connection_id: row.get(19)?,
            evidence_method: row.get(20)?,
            evidence_revision: row.get(21)?,
            auth_context_ref: row.get(22)?,
            source_module_sequence: row.get(23)?,
            source_epoch: row.get(24)?,
            source_observation_id: row.get(25)?,
            details_digest: row.get(26)?,
        })
    }

    fn into_retained(self) -> Result<RetainedCondition> {
        let condition = match self.condition_kind.as_str() {
            "available"
                if self.retry_at_ms.is_none()
                    && self.reset_at_ms.is_none()
                    && self.unknown_native_class.is_none() =>
            {
                ProviderCondition::Available
            }
            "rate_limited" if self.reset_at_ms.is_none() && self.unknown_native_class.is_none() => {
                ProviderCondition::RateLimited {
                    retry_at_ms: self.retry_at_ms,
                }
            }
            "quota_exhausted"
                if self.retry_at_ms.is_none() && self.unknown_native_class.is_none() =>
            {
                ProviderCondition::QuotaExhausted {
                    reset_at_ms: self.reset_at_ms,
                }
            }
            "model_gone"
                if self.retry_at_ms.is_none()
                    && self.reset_at_ms.is_none()
                    && self.unknown_native_class.is_none() =>
            {
                ProviderCondition::ModelGone
            }
            "data_policy_required"
                if self.retry_at_ms.is_none()
                    && self.reset_at_ms.is_none()
                    && self.unknown_native_class.is_none() =>
            {
                ProviderCondition::DataPolicyRequired
            }
            "auth_required"
                if self.retry_at_ms.is_none()
                    && self.reset_at_ms.is_none()
                    && self.unknown_native_class.is_none() =>
            {
                ProviderCondition::AuthRequired
            }
            "overloaded" if self.reset_at_ms.is_none() && self.unknown_native_class.is_none() => {
                ProviderCondition::Overloaded {
                    retry_at_ms: self.retry_at_ms,
                }
            }
            "unknown" if self.retry_at_ms.is_none() && self.reset_at_ms.is_none() => {
                ProviderCondition::Unknown {
                    native_class: self
                        .unknown_native_class
                        .clone()
                        .ok_or_else(|| damaged("unknown condition has no machine class"))?,
                }
            }
            _ => return Err(damaged("retained provider condition columns disagree")),
        };
        let sequence_kind = match self.sequence_kind.as_str() {
            "native" => ProviderConditionSequenceKind::Native,
            "local_collection_revision" => ProviderConditionSequenceKind::LocalCollectionRevision,
            _ => return Err(damaged("retained provider sequence kind is invalid")),
        };
        let evidence_revision = self
            .evidence_revision
            .map(|value| u64::try_from(value).map_err(|_| damaged("provider revision is invalid")))
            .transpose()?;
        let fact = ProviderConditionFact {
            schema_version: 1,
            binding_id: self.binding_id.clone(),
            binding_generation: self.binding_generation,
            route_alias: self.route_alias.clone(),
            native_scope_key: self.native_scope_key.clone(),
            provider_id: self.provider_id.clone(),
            account_id: self.account_id.clone(),
            model_id: self.model_id.clone(),
            bucket_id: (!self.bucket_id.is_empty()).then(|| self.bucket_id.clone()),
            source_model_slug: self.source_model_slug.clone(),
            condition,
            observed_at_ms: self.observed_at_ms,
            sequence_kind,
            native_sequence: self.native_sequence.clone(),
            source_connection_id: self.source_connection_id.clone(),
            evidence_method: self.evidence_method.clone(),
            evidence_revision,
            auth_context_ref: self.auth_context_ref.clone(),
        };
        fact.validate()
            .map_err(|_| damaged("retained provider fact is invalid"))?;
        let scope = ResourceScope {
            scope_key: self.scope_key,
            route_alias: self.route_alias,
            runtime_id: self.runtime_id,
            service_id: self.service_id,
            provider_id: self.provider_id,
            account_id: self.account_id,
            model_id: self.model_id,
            binding_id: Some(self.binding_id),
            binding_generation: Some(self.binding_generation),
            native_scope_key: Some(self.native_scope_key),
        };
        if self.source_module_sequence < 0
            || self.source_observation_id <= 0
            || self.source_epoch.is_empty()
            || !is_sha256(&self.details_digest)
            || self.details_digest
                != retained_digest(
                    &scope,
                    &fact,
                    self.source_module_sequence,
                    &self.source_epoch,
                    self.source_observation_id,
                )?
        {
            return Err(damaged(
                "retained provider condition digest or order is invalid",
            ));
        }
        Ok(RetainedCondition {
            scope,
            fact,
            source_module_sequence: self.source_module_sequence,
            source_epoch: self.source_epoch,
            source_observation_id: self.source_observation_id,
            details_digest: self.details_digest,
        })
    }
}

/// Build the route's one collision key through R13's canonical helper. A
/// binding identity is required only for the helper's binding-scoped fallback.
pub(super) fn resource_scope(route: &Route, binding: Option<&Value>) -> Result<ResourceScope> {
    let route_value = serde_json::to_value(route)?;
    resource_scope_value(&route_value, binding)
}

fn resource_scope_value(route: &Value, binding: Option<&Value>) -> Result<ResourceScope> {
    let binding_id = binding
        .and_then(|value| value.get("binding_id"))
        .and_then(Value::as_str);
    let generation = binding
        .and_then(|value| value.get("generation"))
        .and_then(Value::as_i64);
    let native_scope_key = binding
        .and_then(|value| value.get("native_scope_key"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let runtime = route["runtime"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::invalid("provider condition route has no runtime"))?;
    let service = route["native_options"]["service_id"]
        .as_str()
        .filter(|value| !value.is_empty());
    if service.is_none() && binding_id.is_none() {
        return Err(Error::invalid(
            "binding-scoped provider route requires its exact binding identity",
        ));
    }
    let key_binding_id = binding_id.unwrap_or("");
    let key = capacity::ScopeKey::from_route(route, key_binding_id);
    let facts = capacity::scope_facts(route, native_scope_key.as_deref(), key_binding_id);
    if facts["scope_key"] != key.as_str() {
        return Err(Error::new(
            "STORE_INVARIANT",
            "capacity scope helper returned inconsistent identities",
        ));
    }
    let route_alias = route["alias"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::invalid("provider condition route has no alias"))?;
    Ok(ResourceScope {
        scope_key: key.as_str().to_owned(),
        route_alias: route_alias.to_owned(),
        runtime_id: runtime.to_owned(),
        service_id: facts["service"].as_str().map(str::to_owned),
        provider_id: facts["provider"].as_str().map(str::to_owned),
        account_id: facts["account"].as_str().map(str::to_owned),
        model_id: route_model_id(route),
        binding_id: binding_id.map(str::to_owned),
        binding_generation: generation,
        native_scope_key,
    })
}

fn route_model_id(route: &Value) -> Option<String> {
    let model = &route["native_options"]["model"];
    model
        .get("modelID")
        .or_else(|| model.get("model_id"))
        .or_else(|| model.get("id"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .or_else(|| model.as_str().filter(|value| !value.is_empty()))
        .map(str::to_owned)
}

pub(super) fn scope_for_binding(binding: &Value) -> Result<ResourceScope> {
    let id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let mut scope = resource_scope_value(&binding["route"], Some(binding))?;
    scope.binding_id = Some(id.to_owned());
    scope.binding_generation = Some(generation);
    Ok(scope)
}

fn scope_matches(left: &ResourceScope, right: &ResourceScope) -> bool {
    left.scope_key == right.scope_key
        && left.route_alias == right.route_alias
        && left.runtime_id == right.runtime_id
        && left.service_id == right.service_id
        && left.provider_id == right.provider_id
        && left.account_id == right.account_id
        && left.model_id == right.model_id
        && left.binding_id == right.binding_id
        && left.binding_generation == right.binding_generation
        && left.native_scope_key == right.native_scope_key
}

fn fact_matches_scope(fact: &ProviderConditionFact, scope: &ResourceScope) -> bool {
    fact.binding_id == scope.binding_id.as_deref().unwrap_or_default()
        && Some(fact.binding_generation) == scope.binding_generation
        && fact.route_alias == scope.route_alias
        && fact.native_scope_key == scope.native_scope_key.as_deref().unwrap_or_default()
        && fact.provider_id == scope.provider_id
        && fact.account_id == scope.account_id
        && fact.model_id == scope.model_id
}

/// Retain typed Codex bucket conditions and the account permission with their
/// own source evidence. A damaged collection may contribute an independently
/// evidenced negative bucket condition, but cannot clear absent rows or
/// establish/recover `Available`. Call inside the accepted `module.observe`
/// transaction.
pub(super) fn observe_usage(
    tx: &Transaction<'_>,
    binding: &Value,
    snapshot: &NativeUsageSnapshot,
    module_sequence: i64,
    observation_id: i64,
) -> Result<ConditionWrite> {
    snapshot
        .validate()
        .map_err(|message| Error::new("NATIVE_USAGE_INVALID", message))?;
    if module_sequence < 0 || observation_id <= 0 {
        return Err(Error::invalid("provider observation order is invalid"));
    }
    let scope = scope_for_binding(binding)?;
    let source_epoch = model::text(&binding["observation"], "bridge_boot_id")?;
    if matches!(
        snapshot.evidence.method.as_str(),
        "account/updated" | "connection/closed"
    ) {
        return invalidate_all(tx, &scope, module_sequence, source_epoch, observation_id);
    }
    let collection_issue = snapshot.collection_issue.is_some();
    if snapshot.freshness != UsageFreshness::Current
        || snapshot.service != UsageService::Codex
        || scope.native_scope_key.is_none()
        || snapshot.auth_context_ref.is_none()
        || binding["released_at_ms"].is_i64()
        || binding["state"] == "closed"
    {
        return if collection_issue || snapshot.auth_context_ref.is_none() {
            Ok(ConditionWrite::NoAuthoritativeFact)
        } else {
            invalidate_available(tx, &scope, module_sequence, source_epoch, observation_id)
        };
    }

    let mut result = ConditionWrite::NoAuthoritativeFact;
    if !collection_issue && snapshot.completeness == UsageCompleteness::Full {
        let deleted = clear_absent_bucket_facts(
            tx,
            &scope,
            snapshot,
            module_sequence,
            source_epoch,
            observation_id,
        )?;
        if deleted {
            result = ConditionWrite::Updated;
        }
    }

    for bucket in &snapshot.buckets {
        let Some(observed) = bucket.provider_condition.as_ref() else {
            continue;
        };
        let fact = ProviderConditionFact {
            schema_version: 1,
            binding_id: scope.binding_id.clone().unwrap_or_default(),
            binding_generation: scope.binding_generation.unwrap_or_default(),
            route_alias: scope.route_alias.clone(),
            native_scope_key: scope.native_scope_key.clone().unwrap_or_default(),
            provider_id: scope.provider_id.clone(),
            account_id: scope.account_id.clone(),
            model_id: scope.model_id.clone(),
            bucket_id: Some(bucket.id.clone()),
            source_model_slug: bucket
                .normal_model_slug
                .as_ref()
                .map(|value| value.value.clone()),
            condition: observed.condition.clone(),
            observed_at_ms: observed.collected_at_ms,
            sequence_kind: ProviderConditionSequenceKind::LocalCollectionRevision,
            native_sequence: observed.evidence.revision.to_string(),
            source_connection_id: Some(snapshot.connection_id.clone()),
            evidence_method: Some(observed.evidence.method.clone()),
            evidence_revision: Some(observed.evidence.revision),
            auth_context_ref: snapshot.auth_context_ref.clone(),
        };
        result = combine_write(
            result,
            retain_fact(
                tx,
                &scope,
                &fact,
                module_sequence,
                source_epoch,
                observation_id,
            )?,
        );
    }

    // The native condition on a rolling bucket has its own method, revision,
    // connection and timestamp. It can be retained above even when an
    // optional full read failed. That failure is not evidence of recovery, so
    // preserve any prior permission fact and let its source-current check hold
    // admission while the permission read is unavailable.
    if collection_issue {
        return Ok(result);
    }

    let permission_result = match snapshot.ordinary_usage_allowed.as_ref() {
        Some(permission)
            if permission.evidence.method == "account/rateLimits/read"
                && permission.evidence.revision > 0
                && snapshot.auth_context_ref.is_some()
                && scope.native_scope_key.is_some() =>
        {
            let fact = ProviderConditionFact {
                schema_version: 1,
                binding_id: scope.binding_id.clone().unwrap_or_default(),
                binding_generation: scope.binding_generation.unwrap_or_default(),
                route_alias: scope.route_alias.clone(),
                native_scope_key: scope.native_scope_key.clone().unwrap_or_default(),
                provider_id: scope.provider_id.clone(),
                account_id: scope.account_id.clone(),
                model_id: scope.model_id.clone(),
                bucket_id: None,
                source_model_slug: None,
                condition: if permission.value {
                    ProviderCondition::Available
                } else {
                    ProviderCondition::Unknown {
                        native_class: "ordinary_usage_not_allowed".into(),
                    }
                },
                observed_at_ms: permission.collected_at_ms,
                sequence_kind: ProviderConditionSequenceKind::LocalCollectionRevision,
                native_sequence: permission.evidence.revision.to_string(),
                source_connection_id: Some(snapshot.connection_id.clone()),
                evidence_method: Some(permission.evidence.method.clone()),
                evidence_revision: Some(permission.evidence.revision),
                auth_context_ref: snapshot.auth_context_ref.clone(),
            };
            retain_fact(
                tx,
                &scope,
                &fact,
                module_sequence,
                source_epoch,
                observation_id,
            )?
        }
        _ => invalidate_available(tx, &scope, module_sequence, source_epoch, observation_id)?,
    };
    Ok(combine_write(result, permission_result))
}

fn combine_write(left: ConditionWrite, right: ConditionWrite) -> ConditionWrite {
    match (left, right) {
        (ConditionWrite::Updated, _) | (_, ConditionWrite::Updated) => ConditionWrite::Updated,
        (ConditionWrite::InvalidatedAvailable, _) | (_, ConditionWrite::InvalidatedAvailable) => {
            ConditionWrite::InvalidatedAvailable
        }
        (ConditionWrite::Unchanged, _) | (_, ConditionWrite::Unchanged) => {
            ConditionWrite::Unchanged
        }
        _ => ConditionWrite::NoAuthoritativeFact,
    }
}

/// Retain a producer-classified native condition. The producer still must
/// enter through an authenticated Store observation; this function performs
/// exact route/binding checks and monotonic Store-order checks.
pub(super) fn retain_fact(
    tx: &Transaction<'_>,
    scope: &ResourceScope,
    fact: &ProviderConditionFact,
    module_sequence: i64,
    source_epoch: &str,
    observation_id: i64,
) -> Result<ConditionWrite> {
    fact.validate()
        .map_err(|message| Error::new("PROVIDER_CONDITION_INVALID", message))?;
    if !fact_matches_scope(fact, scope)
        || module_sequence < 0
        || observation_id <= 0
        || !token(source_epoch, 256)
    {
        return Err(Error::new(
            "PROVIDER_CONDITION_SCOPE_MISMATCH",
            "provider condition does not match its exact route and binding",
        ));
    }
    let details_digest =
        retained_digest(scope, fact, module_sequence, source_epoch, observation_id)?;
    let (Some(binding_id), Some(generation)) =
        (scope.binding_id.as_deref(), scope.binding_generation)
    else {
        return Err(Error::new(
            "PROVIDER_CONDITION_SCOPE_MISMATCH",
            "retained provider condition requires an exact binding",
        ));
    };
    let previous = load_retained(
        tx,
        &scope.scope_key,
        binding_id,
        generation,
        fact.bucket_id.as_deref(),
    )?;
    if let Some(previous) = previous {
        if observation_id < previous.source_observation_id {
            return Ok(ConditionWrite::Unchanged);
        }
        if observation_id == previous.source_observation_id {
            if details_digest == previous.details_digest {
                return Ok(ConditionWrite::Unchanged);
            }
            return Err(Error::conflict(
                "provider observation sequence was reused for another fact",
            ));
        }
        if source_epoch == previous.source_epoch {
            if module_sequence < previous.source_module_sequence {
                return Ok(ConditionWrite::Unchanged);
            }
            if module_sequence == previous.source_module_sequence {
                if fact_digest(scope, fact)? == fact_digest(&previous.scope, &previous.fact)? {
                    return Ok(ConditionWrite::Unchanged);
                }
                return Err(Error::conflict(
                    "provider module sequence was reused for another fact",
                ));
            }
        }
        if fact.sequence_kind == ProviderConditionSequenceKind::LocalCollectionRevision
            && previous.fact.sequence_kind == ProviderConditionSequenceKind::LocalCollectionRevision
            && fact.source_connection_id == previous.fact.source_connection_id
        {
            let incoming_revision = fact.evidence_revision.unwrap_or_default();
            let previous_revision = previous.fact.evidence_revision.unwrap_or_default();
            if incoming_revision < previous_revision {
                return Ok(ConditionWrite::Unchanged);
            }
            if incoming_revision == previous_revision {
                if fact_digest(scope, fact)? == fact_digest(&previous.scope, &previous.fact)? {
                    return Ok(ConditionWrite::Unchanged);
                }
                if same_condition_except_model_slug(fact, &previous.fact) {
                    update_retained(
                        tx,
                        scope,
                        fact,
                        module_sequence,
                        source_epoch,
                        observation_id,
                        &details_digest,
                        &previous,
                    )?;
                    return Ok(ConditionWrite::Updated);
                }
                return Err(Error::conflict(
                    "provider read revision was reused for another fact",
                ));
            }
        }
        update_retained(
            tx,
            scope,
            fact,
            module_sequence,
            source_epoch,
            observation_id,
            &details_digest,
            &previous,
        )?;
        return Ok(ConditionWrite::Updated);
    }
    insert_retained(
        tx,
        scope,
        fact,
        module_sequence,
        source_epoch,
        observation_id,
        &details_digest,
    )?;
    Ok(ConditionWrite::Updated)
}

fn same_condition_except_model_slug(
    incoming: &ProviderConditionFact,
    previous: &ProviderConditionFact,
) -> bool {
    let mut incoming = incoming.clone();
    let mut previous = previous.clone();
    incoming.source_model_slug = None;
    previous.source_model_slug = None;
    incoming == previous
}

fn invalidate_available(
    tx: &Transaction<'_>,
    scope: &ResourceScope,
    module_sequence: i64,
    source_epoch: &str,
    observation_id: i64,
) -> Result<ConditionWrite> {
    let (Some(binding_id), Some(generation)) =
        (scope.binding_id.as_deref(), scope.binding_generation)
    else {
        return Ok(ConditionWrite::NoAuthoritativeFact);
    };
    let previous = load_retained(tx, &scope.scope_key, binding_id, generation, None)?;
    let Some(previous) = previous else {
        return Ok(ConditionWrite::NoAuthoritativeFact);
    };
    if !matches!(&previous.fact.condition, ProviderCondition::Available)
        || !may_supersede(
            &previous,
            module_sequence,
            source_epoch,
            observation_id,
            None,
            None,
        )?
    {
        return Ok(ConditionWrite::NoAuthoritativeFact);
    }
    delete_exact(tx, &previous)?;
    Ok(ConditionWrite::InvalidatedAvailable)
}

fn invalidate_all(
    tx: &Transaction<'_>,
    scope: &ResourceScope,
    module_sequence: i64,
    source_epoch: &str,
    observation_id: i64,
) -> Result<ConditionWrite> {
    let (Some(binding_id), Some(generation)) =
        (scope.binding_id.as_deref(), scope.binding_generation)
    else {
        return Ok(ConditionWrite::NoAuthoritativeFact);
    };
    let rows = load_retained_for_binding(tx, &scope.scope_key, binding_id, generation)?;
    let mut changed = false;
    for previous in rows {
        if may_supersede(
            &previous,
            module_sequence,
            source_epoch,
            observation_id,
            None,
            None,
        )? {
            delete_exact(tx, &previous)?;
            changed = true;
        }
    }
    Ok(if changed {
        ConditionWrite::Updated
    } else {
        ConditionWrite::NoAuthoritativeFact
    })
}

fn clear_absent_bucket_facts(
    tx: &Transaction<'_>,
    scope: &ResourceScope,
    snapshot: &NativeUsageSnapshot,
    module_sequence: i64,
    source_epoch: &str,
    observation_id: i64,
) -> Result<bool> {
    let (Some(binding_id), Some(generation)) =
        (scope.binding_id.as_deref(), scope.binding_generation)
    else {
        return Ok(false);
    };
    let present: std::collections::BTreeSet<&str> = snapshot
        .buckets
        .iter()
        .filter(|bucket| bucket.provider_condition.is_some())
        .map(|bucket| bucket.id.as_str())
        .collect();
    let rows = load_retained_for_binding(tx, &scope.scope_key, binding_id, generation)?;
    let mut changed = false;
    for previous in rows {
        let Some(bucket_id) = previous.fact.bucket_id.as_deref() else {
            continue;
        };
        if present.contains(bucket_id) {
            continue;
        }
        if may_supersede(
            &previous,
            module_sequence,
            source_epoch,
            observation_id,
            Some(&snapshot.connection_id),
            Some(snapshot.evidence.revision),
        )? {
            delete_exact(tx, &previous)?;
            changed = true;
        }
    }
    Ok(changed)
}

fn may_supersede(
    previous: &RetainedCondition,
    module_sequence: i64,
    source_epoch: &str,
    observation_id: i64,
    source_connection_id: Option<&str>,
    evidence_revision: Option<u64>,
) -> Result<bool> {
    if observation_id < previous.source_observation_id {
        return Ok(false);
    }
    if source_epoch == previous.source_epoch {
        if module_sequence < previous.source_module_sequence {
            return Ok(false);
        }
        if module_sequence == previous.source_module_sequence {
            if observation_id == previous.source_observation_id {
                return Ok(false);
            }
            return Err(Error::conflict(
                "provider module sequence was reused for a different condition event",
            ));
        }
    }
    if let (Some(connection_id), Some(revision)) = (source_connection_id, evidence_revision)
        && previous.fact.source_connection_id.as_deref() == Some(connection_id)
        && previous.fact.sequence_kind == ProviderConditionSequenceKind::LocalCollectionRevision
    {
        let previous_revision = previous.fact.evidence_revision.unwrap_or_default();
        if revision < previous_revision {
            return Ok(false);
        }
        if revision == previous_revision {
            return Err(Error::conflict(
                "provider read revision was reused while clearing a bucket condition",
            ));
        }
    }
    Ok(true)
}

fn delete_exact(tx: &Transaction<'_>, previous: &RetainedCondition) -> Result<()> {
    let bucket_id = previous.fact.bucket_id.as_deref().unwrap_or("");
    let changed = tx.execute(
        "DELETE FROM provider_conditions WHERE scope_key=?1 AND binding_id=?2 AND binding_generation=?3 AND bucket_id=?4 AND source_observation_id=?5 AND details_digest=?6",
        params![previous.scope.scope_key,previous.scope.binding_id,previous.scope.binding_generation,bucket_id,previous.source_observation_id,previous.details_digest],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "provider condition changed before its exact invalidation",
        ));
    }
    Ok(())
}

/// The one route predicate used by admission and read-only projections.
/// Routes without a service identity remain condition-unobserved until bound;
/// explicit `runtime:service` scopes may use a current fact from another exact
/// source binding. No placeholder binding key is created. `root_claims` must
/// come from R13's exact route attribution.
pub(super) fn check_route(
    db: &Connection,
    route: &Route,
    binding: Option<&Value>,
    root_claims: Option<u32>,
    now_ms: i64,
) -> Result<RouteAdmissionProjection> {
    let route_value = serde_json::to_value(route)?;
    let scope = match binding {
        Some(binding) => Some(scope_for_binding(binding)?),
        None if route_value["native_options"]["service_id"]
            .as_str()
            .is_some_and(|service_id| !service_id.is_empty()) =>
        {
            // A configured service ID gives R13's canonical runtime:service
            // scope before the new root has a binding. No placeholder binding
            // identity is introduced.
            Some(resource_scope_value(&route_value, None)?)
        }
        None => None,
    };
    route_admission(db, route, scope.as_ref(), root_claims, now_ms)
}

fn route_admission(
    db: &Connection,
    route: &Route,
    scope: Option<&ResourceScope>,
    root_claims: Option<u32>,
    now_ms: i64,
) -> Result<RouteAdmissionProjection> {
    if now_ms < 0 {
        return Err(Error::invalid("route admission time is invalid"));
    }
    let root_limit = root_limit_projection(route, root_claims);
    if !route.enabled {
        return Ok(projection(
            RouteAdmissionDecision::Unavailable {
                code: "ROUTE_DISABLED_OR_MISSING".into(),
            },
            ConditionProjection::ConditionUnobserved,
            root_limit,
        ));
    }
    if let Some(policy) = &route.admission_policy {
        if policy.max_concurrent_roots == 0 {
            return Ok(projection(
                RouteAdmissionDecision::Unavailable {
                    code: "ROUTE_POLICY_INVALID".into(),
                },
                ConditionProjection::ConditionUnobserved,
                root_limit,
            ));
        }
        let Some(root_claims) = root_claims else {
            return Ok(projection(
                RouteAdmissionDecision::Hold {
                    code: "ROUTE_CAPACITY_UNKNOWN".into(),
                    until_ms: None,
                },
                ConditionProjection::ConditionUnobserved,
                root_limit,
            ));
        };
        if root_claims >= u32::from(policy.max_concurrent_roots) {
            return Ok(projection(
                RouteAdmissionDecision::Hold {
                    code: "ROUTE_ROOT_LIMIT_REACHED".into(),
                    until_ms: None,
                },
                ConditionProjection::ConditionUnobserved,
                root_limit,
            ));
        }
    }
    let Some(scope) = scope else {
        return Ok(projection(
            RouteAdmissionDecision::Admit,
            ConditionProjection::ConditionUnobserved,
            root_limit,
        ));
    };
    let route_value = serde_json::to_value(route)?;
    let expected = resource_scope_value(
        &route_value,
        Some(&json!({
            "binding_id":scope.binding_id,
            "generation":scope.binding_generation,
            "native_scope_key":scope.native_scope_key,
        })),
    )?;
    if !scope_route_matches(&expected, scope) {
        return Err(Error::new(
            "PROVIDER_CONDITION_SCOPE_MISMATCH",
            "route admission received a scope from another route",
        ));
    }
    let shared_resource = shared_resource_scope_is_exact(scope);
    let retained = if shared_resource {
        // R40 scopes a condition to the canonical shared resource. The retained
        // row still names its source binding; the loop below requires that
        // binding and its current native snapshot to prove the exact scope.
        load_retained_for_scope(db, &scope.scope_key)?
    } else if let (Some(binding_id), Some(generation)) =
        (scope.binding_id.as_deref(), scope.binding_generation)
    {
        load_retained_for_binding(db, &scope.scope_key, binding_id, generation)?
    } else {
        Vec::new()
    };
    if retained.is_empty() {
        return Ok(projection(
            RouteAdmissionDecision::Admit,
            ConditionProjection::ConditionUnobserved,
            root_limit,
        ));
    }

    let mut source_request = scope.clone();
    if shared_resource {
        // A shared runtime:service resource is independent of the prospective
        // root's binding generation. Its source binding is proved separately
        // below against the retained row and current native snapshot.
        source_request.binding_id = None;
        source_request.binding_generation = None;
        source_request.native_scope_key = None;
    }
    let mut selected: Option<(u8, RouteAdmissionDecision, ConditionProjection)> = None;
    for retained in retained {
        if !scope_matches_request(&retained.scope, &source_request)
            || !fact_matches_scope(&retained.fact, &retained.scope)
        {
            continue;
        }
        let same_source_binding = scope.binding_id == retained.scope.binding_id
            && scope.binding_generation == retained.scope.binding_generation;
        if shared_resource && !same_source_binding && retained.fact.auth_context_ref.is_none() {
            // Cross-binding reuse requires evidence of the actual native
            // account. Exact nullable route fields alone do not prove it.
            continue;
        }
        let model_attribution_unknown = match (
            retained.fact.source_model_slug.as_deref(),
            scope.model_id.as_deref(),
        ) {
            (Some(source), Some(route_model)) if source != route_model => continue,
            (Some(_), None) => true,
            _ => false,
        };
        let source_bound = source_binding_matches(db, &retained, &source_request)?;
        let source_current = if !source_bound {
            false
        } else {
            condition_source_is_current(db, &retained)?
        };
        let (decision, rank) = if model_attribution_unknown {
            (
                RouteAdmissionDecision::Hold {
                    code: "PROVIDER_CONDITION_ATTRIBUTION_UNKNOWN".into(),
                    until_ms: None,
                },
                3,
            )
        } else if !source_current {
            (
                RouteAdmissionDecision::Hold {
                    code: "PROVIDER_CONDITION_SOURCE_STALE".into(),
                    until_ms: None,
                },
                2,
            )
        } else {
            let decision = condition_decision(&retained.fact.condition, now_ms);
            let rank = match &decision {
                RouteAdmissionDecision::Admit => 1,
                RouteAdmissionDecision::Hold { .. } => 3,
                RouteAdmissionDecision::Unavailable { .. } => 4,
            };
            (decision, rank)
        };
        let condition = ConditionProjection::Observed {
            condition: retained.fact.condition.clone(),
            observed_at_ms: retained.fact.observed_at_ms,
            source_observation_id: retained.source_observation_id,
            bucket_id: retained.fact.bucket_id.clone(),
            source_model_slug: retained.fact.source_model_slug.clone(),
        };
        if selected
            .as_ref()
            .is_none_or(|(best_rank, _, best_condition)| {
                rank > *best_rank
                    || (rank == *best_rank
                        && retained.source_observation_id
                            > match best_condition {
                                ConditionProjection::Observed {
                                    source_observation_id,
                                    ..
                                } => *source_observation_id,
                                ConditionProjection::ConditionUnobserved => 0,
                            })
            })
        {
            selected = Some((rank, decision, condition));
        }
    }
    if let Some((_, decision, condition)) = selected {
        Ok(projection(decision, condition, root_limit))
    } else {
        Ok(projection(
            RouteAdmissionDecision::Admit,
            ConditionProjection::ConditionUnobserved,
            root_limit,
        ))
    }
}

fn condition_decision(condition: &ProviderCondition, now_ms: i64) -> RouteAdmissionDecision {
    match condition {
        ProviderCondition::Available => RouteAdmissionDecision::Admit,
        ProviderCondition::RateLimited { retry_at_ms } => RouteAdmissionDecision::Hold {
            code: "PROVIDER_RATE_LIMITED".into(),
            until_ms: retry_at_ms.filter(|time| *time > now_ms),
        },
        ProviderCondition::Overloaded { retry_at_ms } => RouteAdmissionDecision::Hold {
            code: "PROVIDER_OVERLOADED".into(),
            until_ms: retry_at_ms.filter(|time| *time > now_ms),
        },
        ProviderCondition::QuotaExhausted { reset_at_ms } => RouteAdmissionDecision::Hold {
            code: "PROVIDER_QUOTA_EXHAUSTED".into(),
            until_ms: reset_at_ms.filter(|time| *time > now_ms),
        },
        ProviderCondition::ModelGone => RouteAdmissionDecision::Unavailable {
            code: "MODEL_GONE".into(),
        },
        ProviderCondition::DataPolicyRequired => RouteAdmissionDecision::Unavailable {
            code: "DATA_POLICY_REQUIRED".into(),
        },
        ProviderCondition::AuthRequired => RouteAdmissionDecision::Unavailable {
            code: "AUTH_REQUIRED".into(),
        },
        ProviderCondition::Unknown { .. } => RouteAdmissionDecision::Hold {
            code: "PROVIDER_CONDITION_UNKNOWN".into(),
            until_ms: None,
        },
    }
}

fn projection(
    decision: RouteAdmissionDecision,
    condition: ConditionProjection,
    root_limit: RootLimitProjection,
) -> RouteAdmissionProjection {
    RouteAdmissionProjection {
        decision,
        condition,
        root_limit,
    }
}

fn root_limit_projection(route: &Route, root_claims: Option<u32>) -> RootLimitProjection {
    let Some(policy) = &route.admission_policy else {
        return RootLimitProjection::LimitNotConfigured;
    };
    let Some(current_claims) = root_claims else {
        return RootLimitProjection::CapacityUnknown {
            max_concurrent_roots: policy.max_concurrent_roots,
        };
    };
    if current_claims >= u32::from(policy.max_concurrent_roots) {
        RootLimitProjection::LimitReached {
            max_concurrent_roots: policy.max_concurrent_roots,
            current_claims,
        }
    } else {
        RootLimitProjection::WithinLimit {
            max_concurrent_roots: policy.max_concurrent_roots,
            current_claims,
        }
    }
}

fn scope_route_matches(expected: &ResourceScope, provided: &ResourceScope) -> bool {
    expected.scope_key == provided.scope_key
        && expected.route_alias == provided.route_alias
        && expected.runtime_id == provided.runtime_id
        && expected.service_id == provided.service_id
        && expected.provider_id == provided.provider_id
        && expected.account_id == provided.account_id
        && expected.model_id == provided.model_id
}

fn scope_matches_request(retained: &ResourceScope, requested: &ResourceScope) -> bool {
    scope_route_matches(retained, requested)
        && requested
            .binding_id
            .as_ref()
            .is_none_or(|value| retained.binding_id.as_ref() == Some(value))
        && requested
            .binding_generation
            .is_none_or(|value| retained.binding_generation == Some(value))
        && requested
            .native_scope_key
            .as_ref()
            .is_none_or(|value| retained.native_scope_key.as_ref() == Some(value))
}

fn shared_resource_scope_is_exact(scope: &ResourceScope) -> bool {
    scope.service_id.is_some()
        && scope.provider_id.is_some()
        && scope.account_id.is_some()
        && scope.model_id.is_some()
}

fn source_binding_matches(
    db: &Connection,
    retained: &RetainedCondition,
    requested: &ResourceScope,
) -> Result<bool> {
    let Some(binding_id) = retained.scope.binding_id.as_deref() else {
        return Ok(false);
    };
    let Some(generation) = retained.scope.binding_generation else {
        return Ok(false);
    };
    let raw: Option<(String, String, Option<String>, Option<i64>)> = db
        .query_row(
            "SELECT route_json,state_json,native_scope_key,released_at_ms FROM bindings WHERE binding_id=?1 AND generation=?2",
            params![binding_id,generation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((route_json, state_json, native_scope_key, released_at_ms)) = raw else {
        return Ok(false);
    };
    if released_at_ms.is_some() {
        return Ok(false);
    }
    let route: Value = serde_json::from_str(&route_json)?;
    let state: Value = serde_json::from_str(&state_json)?;
    let binding_value = json!({
        "binding_id":binding_id,
        "generation":generation,
        "native_scope_key":native_scope_key,
    });
    let current_scope = resource_scope_value(&route, Some(&binding_value))?;
    Ok(scope_matches(&current_scope, &retained.scope)
        && scope_matches_request(&retained.scope, requested)
        && state["bridge_boot_id"] == retained.source_epoch)
}

fn condition_source_is_current(db: &Connection, retained: &RetainedCondition) -> Result<bool> {
    if retained.fact.bucket_id.is_some() {
        bucket_condition_source_is_current(db, retained)
    } else {
        permission_source_is_current(db, retained)
    }
}

fn permission_source_is_current(db: &Connection, retained: &RetainedCondition) -> Result<bool> {
    let (Some(binding_id), Some(generation)) = (
        retained.scope.binding_id.as_deref(),
        retained.scope.binding_generation,
    ) else {
        return Ok(false);
    };
    let raw: Option<(String, String, Option<i64>)> = db
        .query_row(
            "SELECT state,state_json,released_at_ms FROM bindings WHERE binding_id=?1 AND generation=?2",
            params![binding_id,generation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((state_name, state_json, released_at_ms)) = raw else {
        return Ok(false);
    };
    if released_at_ms.is_some() || state_name == "closed" {
        return Ok(false);
    }
    let state: Value = serde_json::from_str(&state_json)?;
    if state["connection"] != "connected"
        || state["bridge_boot_id"] != retained.source_epoch
        || state["native_usage"]["boot_id"] != retained.source_epoch
    {
        return Ok(false);
    }
    let snapshot: NativeUsageSnapshot =
        serde_json::from_value(state["native_usage"]["snapshot"].clone())
            .map_err(|_| damaged("current native usage permission snapshot is invalid"))?;
    if snapshot.validate().is_err()
        || snapshot.freshness != UsageFreshness::Current
        || snapshot.collection_issue.is_some()
    {
        return Ok(false);
    }
    let Some(permission) = snapshot.ordinary_usage_allowed.as_ref() else {
        return Ok(false);
    };
    let expected_value = match &retained.fact.condition {
        ProviderCondition::Available => true,
        ProviderCondition::Unknown { native_class }
            if native_class == "ordinary_usage_not_allowed" =>
        {
            false
        }
        _ => return Ok(false),
    };
    Ok(snapshot.service == UsageService::Codex
        && permission.value == expected_value
        && snapshot.connection_id
            == retained
                .fact
                .source_connection_id
                .as_deref()
                .unwrap_or_default()
        && snapshot.auth_context_ref == retained.fact.auth_context_ref
        && permission.evidence.method == "account/rateLimits/read"
        && Some(permission.evidence.revision) == retained.fact.evidence_revision
        && permission.collected_at_ms == retained.fact.observed_at_ms)
}

fn bucket_condition_source_is_current(
    db: &Connection,
    retained: &RetainedCondition,
) -> Result<bool> {
    let (Some(binding_id), Some(generation), Some(bucket_id)) = (
        retained.scope.binding_id.as_deref(),
        retained.scope.binding_generation,
        retained.fact.bucket_id.as_deref(),
    ) else {
        return Ok(false);
    };
    let raw: Option<(String, String)> = db
        .query_row(
            "SELECT state_json,bridge_boot_id FROM bindings WHERE binding_id=?1 AND generation=?2",
            params![binding_id, generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((state_json, bridge_boot_id)) = raw else {
        return Ok(false);
    };
    if bridge_boot_id != retained.source_epoch {
        return Ok(false);
    }
    let state: Value = serde_json::from_str(&state_json)?;
    if state["native_usage"]["boot_id"] != retained.source_epoch {
        return Ok(false);
    }
    let snapshot: NativeUsageSnapshot =
        serde_json::from_value(state["native_usage"]["snapshot"].clone())
            .map_err(|_| damaged("current native usage condition snapshot is invalid"))?;
    if snapshot.validate().is_err()
        || snapshot.service != UsageService::Codex
        || snapshot.auth_context_ref.is_none()
        || snapshot.connection_id
            != retained
                .fact
                .source_connection_id
                .as_deref()
                .unwrap_or_default()
        || snapshot.auth_context_ref != retained.fact.auth_context_ref
    {
        return Ok(false);
    }
    let Some(bucket) = snapshot
        .buckets
        .iter()
        .find(|bucket| bucket.id == bucket_id)
    else {
        return Ok(false);
    };
    let Some(observed) = bucket.provider_condition.as_ref() else {
        return Ok(false);
    };
    Ok(observed.condition == retained.fact.condition
        && observed.collected_at_ms == retained.fact.observed_at_ms
        && observed.evidence.method == retained.fact.evidence_method.as_deref().unwrap_or_default()
        && Some(observed.evidence.revision) == retained.fact.evidence_revision
        && retained
            .fact
            .source_model_slug
            .as_deref()
            .is_none_or(|slug| {
                bucket
                    .normal_model_slug
                    .as_ref()
                    .is_some_and(|value| value.value == slug)
            }))
}

fn load_retained(
    db: &Connection,
    scope_key: &str,
    binding_id: &str,
    generation: i64,
    bucket_id: Option<&str>,
) -> Result<Option<RetainedCondition>> {
    let stored = db
        .query_row(
            "SELECT scope_key,bucket_id,source_model_slug,route_alias,runtime_id,service_id,binding_id,binding_generation,native_scope_key,provider_id,account_id,model_id,condition_kind,retry_at_ms,reset_at_ms,unknown_native_class,observed_at_ms,sequence_kind,native_sequence,source_connection_id,evidence_method,evidence_revision,auth_context_ref,source_module_sequence,source_epoch,source_observation_id,details_digest FROM provider_conditions WHERE scope_key=?1 AND binding_id=?2 AND binding_generation=?3 AND bucket_id=?4",
            params![scope_key,binding_id,generation,bucket_id.unwrap_or("")],
            StoredCondition::read,
        )
        .optional()?;
    stored.map(StoredCondition::into_retained).transpose()
}

fn load_retained_for_binding(
    db: &Connection,
    scope_key: &str,
    binding_id: &str,
    generation: i64,
) -> Result<Vec<RetainedCondition>> {
    let mut statement = db.prepare(
        "SELECT scope_key,bucket_id,source_model_slug,route_alias,runtime_id,service_id,binding_id,binding_generation,native_scope_key,provider_id,account_id,model_id,condition_kind,retry_at_ms,reset_at_ms,unknown_native_class,observed_at_ms,sequence_kind,native_sequence,source_connection_id,evidence_method,evidence_revision,auth_context_ref,source_module_sequence,source_epoch,source_observation_id,details_digest FROM provider_conditions WHERE scope_key=?1 AND binding_id=?2 AND binding_generation=?3 ORDER BY bucket_id",
    )?;
    let rows = statement.query_map(
        params![scope_key, binding_id, generation],
        StoredCondition::read,
    )?;
    rows.map(|row| row?.into_retained()).collect()
}

fn load_retained_for_scope(db: &Connection, scope_key: &str) -> Result<Vec<RetainedCondition>> {
    let mut statement = db.prepare(
        "SELECT scope_key,bucket_id,source_model_slug,route_alias,runtime_id,service_id,binding_id,binding_generation,native_scope_key,provider_id,account_id,model_id,condition_kind,retry_at_ms,reset_at_ms,unknown_native_class,observed_at_ms,sequence_kind,native_sequence,source_connection_id,evidence_method,evidence_revision,auth_context_ref,source_module_sequence,source_epoch,source_observation_id,details_digest FROM provider_conditions WHERE scope_key=?1 ORDER BY source_observation_id,binding_id,binding_generation,bucket_id",
    )?;
    let rows = statement.query_map(params![scope_key], StoredCondition::read)?;
    rows.map(|row| row?.into_retained()).collect()
}

fn retained_digest(
    scope: &ResourceScope,
    fact: &ProviderConditionFact,
    module_sequence: i64,
    source_epoch: &str,
    observation_id: i64,
) -> Result<String> {
    let identity = json!({
        "scope":scope,
        "fact":fact,
        "source_module_sequence":module_sequence,
        "source_epoch":source_epoch,
        "source_observation_id":observation_id,
    });
    Ok(model::digest(model::canonical(&identity)?.as_bytes()))
}

fn fact_digest(scope: &ResourceScope, fact: &ProviderConditionFact) -> Result<String> {
    let value = json!({"scope":scope,"fact":fact});
    Ok(model::digest(model::canonical(&value)?.as_bytes()))
}

fn condition_columns(
    fact: &ProviderConditionFact,
) -> (&'static str, Option<i64>, Option<i64>, Option<&str>) {
    match &fact.condition {
        ProviderCondition::Available => ("available", None, None, None),
        ProviderCondition::RateLimited { retry_at_ms } => {
            ("rate_limited", *retry_at_ms, None, None)
        }
        ProviderCondition::QuotaExhausted { reset_at_ms } => {
            ("quota_exhausted", None, *reset_at_ms, None)
        }
        ProviderCondition::ModelGone => ("model_gone", None, None, None),
        ProviderCondition::DataPolicyRequired => ("data_policy_required", None, None, None),
        ProviderCondition::AuthRequired => ("auth_required", None, None, None),
        ProviderCondition::Overloaded { retry_at_ms } => ("overloaded", *retry_at_ms, None, None),
        ProviderCondition::Unknown { native_class } => ("unknown", None, None, Some(native_class)),
    }
}

fn sequence_kind(value: ProviderConditionSequenceKind) -> &'static str {
    match value {
        ProviderConditionSequenceKind::Native => "native",
        ProviderConditionSequenceKind::LocalCollectionRevision => "local_collection_revision",
    }
}

fn fact_evidence_revision(fact: &ProviderConditionFact) -> Result<Option<i64>> {
    fact.evidence_revision
        .map(|value| {
            i64::try_from(value)
                .map_err(|_| Error::invalid("provider evidence revision exceeds Store bounds"))
        })
        .transpose()
}

fn insert_retained(
    tx: &Transaction<'_>,
    scope: &ResourceScope,
    fact: &ProviderConditionFact,
    module_sequence: i64,
    source_epoch: &str,
    observation_id: i64,
    details_digest: &str,
) -> Result<()> {
    let (kind, retry, reset, unknown) = condition_columns(fact);
    let service_id = scope.service_id.as_deref();
    let provider_id = fact.provider_id.as_deref();
    let account_id = fact.account_id.as_deref();
    let model_id = fact.model_id.as_deref();
    let bucket_id = fact.bucket_id.as_deref().unwrap_or("");
    let source_model_slug = fact.source_model_slug.as_deref();
    let source_connection = fact.source_connection_id.as_deref();
    let evidence_method = fact.evidence_method.as_deref();
    let evidence_revision = fact_evidence_revision(fact)?;
    let auth_context = fact.auth_context_ref.as_deref();
    tx.execute(
        "INSERT INTO provider_conditions(scope_key,bucket_id,source_model_slug,route_alias,runtime_id,service_id,binding_id,binding_generation,native_scope_key,provider_id,account_id,model_id,condition_kind,retry_at_ms,reset_at_ms,unknown_native_class,observed_at_ms,sequence_kind,native_sequence,source_connection_id,evidence_method,evidence_revision,auth_context_ref,source_module_sequence,source_epoch,source_observation_id,details_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27)",
        params![scope.scope_key,bucket_id,source_model_slug,fact.route_alias,scope.runtime_id,service_id,fact.binding_id,fact.binding_generation,fact.native_scope_key,provider_id,account_id,model_id,kind,retry,reset,unknown,fact.observed_at_ms,sequence_kind(fact.sequence_kind),fact.native_sequence,source_connection,evidence_method,evidence_revision,auth_context,module_sequence,source_epoch,observation_id,details_digest],
    )?;
    Ok(())
}

fn update_retained(
    tx: &Transaction<'_>,
    scope: &ResourceScope,
    fact: &ProviderConditionFact,
    module_sequence: i64,
    source_epoch: &str,
    observation_id: i64,
    details_digest: &str,
    previous: &RetainedCondition,
) -> Result<()> {
    let (kind, retry, reset, unknown) = condition_columns(fact);
    let service_id = scope.service_id.as_deref();
    let provider_id = fact.provider_id.as_deref();
    let account_id = fact.account_id.as_deref();
    let model_id = fact.model_id.as_deref();
    let bucket_id = fact.bucket_id.as_deref().unwrap_or("");
    let source_model_slug = fact.source_model_slug.as_deref();
    let source_connection = fact.source_connection_id.as_deref();
    let evidence_method = fact.evidence_method.as_deref();
    let evidence_revision = fact_evidence_revision(fact)?;
    let auth_context = fact.auth_context_ref.as_deref();
    let changed = tx.execute(
        "UPDATE provider_conditions SET source_model_slug=?3,route_alias=?4,runtime_id=?5,service_id=?6,native_scope_key=?9,provider_id=?10,account_id=?11,model_id=?12,condition_kind=?13,retry_at_ms=?14,reset_at_ms=?15,unknown_native_class=?16,observed_at_ms=?17,sequence_kind=?18,native_sequence=?19,source_connection_id=?20,evidence_method=?21,evidence_revision=?22,auth_context_ref=?23,source_module_sequence=?24,source_epoch=?25,source_observation_id=?26,details_digest=?27 WHERE scope_key=?1 AND bucket_id=?2 AND binding_id=?7 AND binding_generation=?8 AND source_observation_id=?28 AND details_digest=?29",
        params![scope.scope_key,bucket_id,source_model_slug,fact.route_alias,scope.runtime_id,service_id,fact.binding_id,fact.binding_generation,fact.native_scope_key,provider_id,account_id,model_id,kind,retry,reset,unknown,fact.observed_at_ms,sequence_kind(fact.sequence_kind),fact.native_sequence,source_connection,evidence_method,evidence_revision,auth_context,module_sequence,source_epoch,observation_id,details_digest,previous.source_observation_id,previous.details_digest],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "provider condition changed before its exact current-row update",
        ));
    }
    Ok(())
}

fn damaged(message: &'static str) -> Error {
    Error::new("PROVIDER_CONDITION_DAMAGED", message)
}

fn text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

fn token(value: &str, max: usize) -> bool {
    text(value, max)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
