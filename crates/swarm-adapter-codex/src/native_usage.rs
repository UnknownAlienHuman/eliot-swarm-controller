use super::{NativeRateLimitSnapshot, NativeRateLimitWindow};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use swarm_contracts::native_usage::{
    NativeUsageSnapshot, ObservedProviderCondition, ObservedUsagePermission, ObservedUsageValue,
    UsageBucket, UsageCollectionIssue, UsageCompleteness, UsageCredits, UsageEvidence,
    UsageFreshness, UsageIndividualLimit, UsageService, UsageWindow,
};
use swarm_contracts::provider_condition::ProviderCondition;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FullRead {
    account_id: Option<String>,
    ordinary_usage_allowed: Option<bool>,
    rate_limits: Option<NativeRateLimitSnapshot>,
    rate_limits_by_limit_id: Option<BTreeMap<String, NativeRateLimitSnapshot>>,
}

#[derive(Default)]
pub(super) struct Collector {
    connection_id: Option<String>,
    revision: u64,
    snapshot: Option<NativeUsageSnapshot>,
    auth_context_ref: Option<String>,
    ordinary_usage_allowed: Option<ObservedUsagePermission>,
    refresh_requested: bool,
}

impl Collector {
    pub(super) fn connect(&mut self) {
        self.connection_id = Some(Uuid::new_v4().to_string());
        self.revision = self.revision.saturating_add(1);
        self.snapshot = None;
        self.auth_context_ref = None;
        self.ordinary_usage_allowed = None;
        self.refresh_requested = false;
    }

    pub(super) fn revision(&self) -> u64 {
        self.revision
    }

    pub(super) fn snapshot(&self) -> Option<NativeUsageSnapshot> {
        self.snapshot.clone()
    }

    pub(super) fn invalidate_auth(&mut self) {
        self.revision = self.revision.saturating_add(1);
        self.set_unavailable("account/updated", UsageCompleteness::NotObserved);
        self.refresh_requested = true;
    }

    pub(super) fn take_refresh_revision(&mut self) -> Option<u64> {
        std::mem::take(&mut self.refresh_requested).then_some(self.revision)
    }

    pub(super) fn closed(&mut self) {
        self.revision = self.revision.saturating_add(1);
        if let Some(snapshot) = &mut self.snapshot {
            snapshot.freshness = UsageFreshness::Stale;
            snapshot.evidence.method = "connection/closed".into();
            snapshot.evidence.revision = self.revision;
        }
    }

    pub(super) fn full_read(
        &mut self,
        expected_revision: u64,
        result: Result<serde_json::Value, bool>,
    ) {
        // This marker protects a read/event race; it is not a native timestamp.
        if self.revision != expected_revision {
            self.refresh_requested = true;
            return;
        }
        self.revision = self.revision.saturating_add(1);
        let parsed = match result {
            Ok(value) => serde_json::from_value::<FullRead>(value),
            Err(unsupported) => {
                self.set_unavailable(
                    "account/rateLimits/read",
                    if unsupported {
                        UsageCompleteness::Unsupported
                    } else {
                        UsageCompleteness::NotObserved
                    },
                );
                return;
            }
        };
        let Ok(parsed) = parsed else {
            self.set_unavailable("account/rateLimits/read", UsageCompleteness::Invalid);
            return;
        };
        let at = now_ms();
        let auth_context_ref = match parsed.account_id {
            Some(id) if !id.is_empty() && id.len() <= 1024 && !id.chars().any(char::is_control) => {
                Some(format!("{:x}", Sha256::digest(id.as_bytes())))
            }
            Some(_) => {
                self.set_unavailable("account/rateLimits/read", UsageCompleteness::Invalid);
                return;
            }
            None => None,
        };
        let ordinary_usage_allowed =
            parsed
                .ordinary_usage_allowed
                .map(|value| ObservedUsagePermission {
                    value,
                    collected_at_ms: at,
                    evidence: UsageEvidence {
                        method: "account/rateLimits/read".into(),
                        revision: self.revision,
                        native_observed_at_ms: None,
                    },
                });
        let mut buckets = Vec::new();
        // The map is authoritative when present; the legacy view is not an
        // additional independent budget.
        if let Some(map) = parsed.rate_limits_by_limit_id {
            for (id, limit) in map {
                if limit.limit_id.as_ref().is_some_and(|native| native != &id) {
                    self.set_unavailable("account/rateLimits/read", UsageCompleteness::Invalid);
                    return;
                }
                let Some(bucket) = bucket(
                    id,
                    limit,
                    None,
                    at,
                    &source("account/rateLimits/read", self.revision),
                ) else {
                    self.set_unavailable("account/rateLimits/read", UsageCompleteness::Invalid);
                    return;
                };
                buckets.push(bucket);
            }
        } else if let Some(limit) = parsed.rate_limits {
            // Codex's legacy nullable limitId denotes its default bucket.
            let id = limit
                .limit_id
                .clone()
                .unwrap_or_else(|| "codex-default".into());
            let Some(bucket) = bucket(
                id,
                limit,
                None,
                at,
                &source("account/rateLimits/read", self.revision),
            ) else {
                self.set_unavailable("account/rateLimits/read", UsageCompleteness::Invalid);
                return;
            };
            buckets.push(bucket);
        }
        self.auth_context_ref = auth_context_ref;
        self.ordinary_usage_allowed = ordinary_usage_allowed;
        self.publish(
            "account/rateLimits/read",
            buckets,
            if at > 0 {
                UsageCompleteness::Full
            } else {
                UsageCompleteness::Invalid
            },
            None,
        );
    }

    pub(super) fn rolling_update(&mut self, limit: NativeRateLimitSnapshot) {
        // A negative condition or its next sparse notification needs an
        // authoritative read. Percentages and elapsed resets cannot recover
        // account permission, and this schedules no model/native input.
        self.refresh_requested |= limit.rate_limit_reached_type.is_some()
            || limit.spend_control_reached == Some(true)
            || self
                .ordinary_usage_allowed
                .as_ref()
                .is_some_and(|permission| !permission.value)
            || self.snapshot.as_ref().is_some_and(|snapshot| {
                snapshot.buckets.iter().any(|bucket| {
                    bucket.rate_limit_reached_type.is_some()
                        || bucket.spend_control_reached == Some(true)
                })
            });
        self.revision = self.revision.saturating_add(1);
        let at = now_ms();
        let id = limit
            .limit_id
            .clone()
            .unwrap_or_else(|| "codex-default".into());
        let mut buckets = self
            .snapshot
            .as_ref()
            .filter(|snapshot| {
                snapshot.connection_id == self.connection_id.as_deref().unwrap_or_default()
            })
            .map(|snapshot| snapshot.buckets.clone())
            .unwrap_or_default();
        let old = buckets.iter().find(|bucket| bucket.id == id);
        let Some(updated) = bucket(
            id.clone(),
            limit,
            old,
            at,
            &source("account/rateLimits/updated", self.revision),
        ) else {
            self.set_unavailable("account/rateLimits/updated", UsageCompleteness::Invalid);
            return;
        };
        buckets.retain(|bucket| bucket.id != id);
        buckets.push(updated);
        buckets.sort_by(|left, right| left.id.cmp(&right.id));
        self.publish(
            "account/rateLimits/updated",
            buckets,
            UsageCompleteness::Partial,
            self.snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.collection_issue),
        );
    }

    pub(super) fn malformed_update(&mut self) {
        self.revision = self.revision.saturating_add(1);
        self.set_unavailable("account/rateLimits/updated", UsageCompleteness::Invalid);
    }

    fn set_unavailable(&mut self, method: &str, completeness: UsageCompleteness) {
        if method == "account/updated" {
            self.auth_context_ref = None;
            self.ordinary_usage_allowed = None;
            self.publish(method, Vec::new(), completeness, None);
            return;
        }
        let buckets = self
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.buckets.clone())
            .unwrap_or_default();
        let issue = match completeness {
            UsageCompleteness::Unsupported => UsageCollectionIssue::ReadUnsupported,
            UsageCompleteness::Invalid => UsageCollectionIssue::InvalidPayload,
            _ => UsageCollectionIssue::ReadUnavailable,
        };
        let completeness = if buckets.is_empty() {
            completeness
        } else {
            UsageCompleteness::Partial
        };
        self.publish(method, buckets, completeness, Some(issue));
    }

    fn publish(
        &mut self,
        method: &str,
        buckets: Vec<UsageBucket>,
        mut completeness: UsageCompleteness,
        collection_issue: Option<UsageCollectionIssue>,
    ) {
        let Some(connection_id) = self.connection_id.clone() else {
            return;
        };
        if buckets.is_empty() && completeness == UsageCompleteness::Full {
            completeness = UsageCompleteness::NotObserved;
        }
        let snapshot = NativeUsageSnapshot {
            schema_version: 1,
            service: UsageService::Codex,
            connection_id,
            // Only the backend's associated opaque accountId proves this
            // context; endpoint, plan and email never substitute for it.
            auth_context_ref: self.auth_context_ref.clone(),
            ordinary_usage_allowed: self.ordinary_usage_allowed.clone(),
            collection_issue,
            collected_at_ms: now_ms(),
            freshness: UsageFreshness::Current,
            completeness,
            evidence: UsageEvidence {
                method: method.into(),
                revision: self.revision,
                native_observed_at_ms: None,
            },
            buckets,
        };
        if snapshot.validate().is_ok() {
            self.snapshot = Some(snapshot);
        } else if let Some(snapshot) = &mut self.snapshot {
            snapshot.freshness = UsageFreshness::Stale;
            snapshot.completeness = UsageCompleteness::Invalid;
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|value| i64::try_from(value.as_millis()).ok())
        .unwrap_or(0)
}

fn observed<T>(value: T, at: i64) -> ObservedUsageValue<T> {
    ObservedUsageValue {
        value,
        collected_at_ms: at,
    }
}

fn source(method: &str, revision: u64) -> UsageEvidence {
    UsageEvidence {
        method: method.into(),
        revision,
        native_observed_at_ms: None,
    }
}

fn provider_condition(
    value: &NativeRateLimitSnapshot,
    previous: Option<&UsageBucket>,
    at: i64,
    evidence: &UsageEvidence,
) -> Option<ObservedProviderCondition> {
    let future_reset = |seconds: Option<i64>| {
        seconds
            .and_then(|seconds| seconds.checked_mul(1000))
            .filter(|time| *time >= at)
    };
    let condition = match value.rate_limit_reached_type.as_deref() {
        Some("rate_limit_reached") => ProviderCondition::RateLimited {
            // The native class does not identify which rolling window blocked
            // the request. Keep each reset in its window, without inventing a
            // retry deadline for the condition.
            retry_at_ms: None,
        },
        Some("workspace_owner_credits_depleted" | "workspace_member_credits_depleted") => {
            ProviderCondition::QuotaExhausted { reset_at_ms: None }
        }
        Some("workspace_owner_usage_limit_reached" | "workspace_member_usage_limit_reached") => {
            ProviderCondition::QuotaExhausted {
                reset_at_ms: future_reset(
                    value.individual_limit.as_ref().map(|limit| limit.resets_at),
                ),
            }
        }
        Some(_) => ProviderCondition::Unknown {
            native_class: "unrecognized_rate_limit_class".into(),
        },
        None if value.spend_control_reached == Some(true) => ProviderCondition::QuotaExhausted {
            reset_at_ms: future_reset(value.individual_limit.as_ref().map(|limit| limit.resets_at)),
        },
        None => return previous.and_then(|bucket| bucket.provider_condition.clone()),
    };
    Some(ObservedProviderCondition {
        condition,
        collected_at_ms: at,
        evidence: evidence.clone(),
    })
}

fn window(value: Option<NativeRateLimitWindow>) -> Option<Option<UsageWindow>> {
    let Some(value) = value else {
        return Some(None);
    };
    let resets_at_ms = match value.resets_at {
        Some(value) => Some(value.checked_mul(1000)?),
        None => None,
    };
    Some(Some(UsageWindow {
        collected_at_ms: now_ms(),
        used_percent: i64::from(value.used_percent),
        resets_at_ms,
        window_duration_mins: value.window_duration_mins,
    }))
}

fn bucket(
    id: String,
    value: NativeRateLimitSnapshot,
    old: Option<&UsageBucket>,
    at: i64,
    evidence: &UsageEvidence,
) -> Option<UsageBucket> {
    let provider_condition = provider_condition(&value, old, at, evidence);
    let metadata = |new: Option<String>, previous: Option<&ObservedUsageValue<String>>| {
        new.map(|value| observed(value, at))
            .or_else(|| previous.cloned())
    };
    Some(UsageBucket {
        provider_condition,
        id,
        name: metadata(value.limit_name, old.and_then(|v| v.name.as_ref())),
        normal_model_slug: metadata(
            value.normal_model_slug,
            old.and_then(|v| v.normal_model_slug.as_ref()),
        ),
        plan_type: metadata(value.plan_type, old.and_then(|v| v.plan_type.as_ref())),
        // Missing/reset windows are not metadata: never silently inherit them.
        primary: window(value.primary)?,
        secondary: window(value.secondary)?,
        credits: value
            .credits
            .map(|v| {
                observed(
                    UsageCredits {
                        balance: v.balance,
                        has_credits: v.has_credits,
                        unlimited: v.unlimited,
                    },
                    at,
                )
            })
            .or_else(|| old.and_then(|v| v.credits.clone())),
        individual_limit: match value.individual_limit {
            Some(v) => Some(observed(
                UsageIndividualLimit {
                    limit: v.limit,
                    used: v.used,
                    remaining_percent: i64::from(v.remaining_percent),
                    resets_at_ms: v.resets_at.checked_mul(1000)?,
                },
                at,
            )),
            None => old.and_then(|v| v.individual_limit.clone()),
        },
        spend_control_reached: value.spend_control_reached,
        rate_limit_reached_type: metadata(
            value.rate_limit_reached_type,
            old.and_then(|v| v.rate_limit_reached_type.as_ref()),
        ),
    })
}
