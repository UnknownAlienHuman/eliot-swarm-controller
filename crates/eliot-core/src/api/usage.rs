//! Bounded usage summaries with explicit coverage. One read covers exactly
//! one scope: one Agent (one retained generation or all of them), one
//! project label, or one tree (a root Agent and its descendants). Turn
//! expenditure never merges native scopes: each scope (`turn`,
//! `main_loop_turn`, `message`, or `unknown` when unattributed) reports its
//! own subtotals. Session facts report the latest snapshot per generation in
//! scope; cumulative counters sum only where every included snapshot supplies
//! them, and cost sums only within one verbatim unit, never converted.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// One usage-summary scope: exactly one of `agent`, `project` or `root`.
/// `generation` narrows only the Agent scope to one retained generation.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageSummaryRequest {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub agent: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_generation"
    )]
    #[schemars(range(min = 0))]
    pub generation: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub project: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub root: Option<String>,
}

impl UsageSummaryRequest {
    pub fn validate(&self) -> Result<()> {
        let scopes = [&self.agent, &self.project, &self.root]
            .iter()
            .filter(|scope| scope.is_some())
            .count();
        if scopes != 1 {
            return Err(Error::new(
                "invalid",
                "usage summary needs exactly one of agent, project or root",
            ));
        }
        if let Some(agent) = &self.agent {
            validate_text(agent)?;
            crate::parse_agent_id(agent)?;
        }
        if let Some(project) = &self.project {
            validate_text(project)?;
        }
        if let Some(root) = &self.root {
            validate_text(root)?;
            crate::parse_agent_id(root)?;
        }
        if let Some(generation) = self.generation {
            if generation < 0 {
                return Err(Error::new("invalid", "generation must be nonnegative"));
            }
            if self.agent.is_none() {
                return Err(Error::new(
                    "invalid",
                    "usage summary generation needs the agent scope",
                ));
            }
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &UsageSummaryResponse) -> Result<()> {
        if self.validate().is_err() {
            return Err(invalid_response());
        }
        if reply.scope.agent != self.agent
            || reply.scope.generation != self.generation
            || reply.scope.project != self.project
            || reply.scope.root != self.root
        {
            return Err(invalid_response());
        }
        reply.validate()?;
        Ok(())
    }
}

/// Echo of the requested scope, so a caller can bind the reply.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageScopeView {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
}

/// Known turn-expenditure subtotals for one native scope. A counter is
/// present only when at least one counted turn in the scope supplied it;
/// absent native data stays absent, never zero.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnCounterTotals {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_tokens: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<i64>,
}

/// How many counted turns lacked each turn counter. Turns without any usage
/// row count as missing every counter.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnCounterMissing {
    pub input_tokens: usize,
    pub output_tokens: usize,
    pub reasoning_tokens: usize,
    pub thinking_tokens: usize,
    pub cache_read_tokens: usize,
    pub cache_write_tokens: usize,
    pub total_tokens: usize,
}

/// Turn expenditure for one native attribution scope. Scopes never merge:
/// there is no cross-scope total, only these per-scope subtotals.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnScopeTotals {
    pub scope: String,
    pub turns_counted: usize,
    pub totals: TurnCounterTotals,
    pub turns_missing: TurnCounterMissing,
}

/// Turn expenditure over the retained terminal turns in scope, grouped by
/// native scope. `turns_counted` is every retained terminal turn examined;
/// `turns_without_usage` had no usage row at all.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnExpenditure {
    pub turns_counted: usize,
    pub turns_without_usage: usize,
    pub turns_missing: TurnCounterMissing,
    pub by_scope: Vec<TurnScopeTotals>,
}

/// One cumulative session counter: the subtotal over the snapshots that
/// supplied it, with explicit coverage. `complete` is true only when every
/// included snapshot supplied the counter.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionTokenSubtotal {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<i64>,
    pub supplied: usize,
    pub missing: usize,
    pub complete: bool,
}

/// Cumulative session counters summed only where supplied; a partial sum
/// never reads as complete.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCumulativeTotals {
    pub input_tokens: SessionTokenSubtotal,
    pub output_tokens: SessionTokenSubtotal,
    pub cache_read_tokens: SessionTokenSubtotal,
    pub cache_write_tokens: SessionTokenSubtotal,
}

/// Cumulative session cost within one verbatim native unit. Units never mix
/// and amounts never convert.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionCostTotal {
    pub unit: String,
    pub total_amount: f64,
    pub snapshots: usize,
}

/// Session facts over the latest snapshot per Agent generation in scope.
/// Context occupancy is point-in-time and never sums: only its coverage
/// reads back here.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionFacts {
    pub snapshots: usize,
    pub generations_without_snapshot: usize,
    pub cumulative_tokens: SessionCumulativeTotals,
    pub cost_by_unit: Vec<SessionCostTotal>,
    pub snapshots_without_cost: usize,
    pub context_occupancy_supplied: usize,
    pub context_capacity_supplied: usize,
}

/// Bounded usage totals with explicit coverage. No partial sum presents as
/// complete: turn scopes stay separate, session subtotals carry their
/// missing counts, and `truncated`/`history_complete` say what was left out.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsageSummaryResponse {
    pub scope: UsageScopeView,
    pub agents_included: usize,
    pub generations_included: usize,
    pub truncated: bool,
    pub history_complete: bool,
    pub history_reason: String,
    pub pruned_through: i64,
    pub turns: TurnExpenditure,
    pub sessions: SessionFacts,
}

impl UsageSummaryResponse {
    pub fn validate(&self) -> Result<()> {
        if self.history_reason.trim().is_empty() {
            return Err(invalid_response());
        }
        if self.pruned_through < 0 {
            return Err(invalid_response());
        }
        if self.generations_included < self.sessions.snapshots {
            return Err(invalid_response());
        }
        if self.sessions.snapshots + self.sessions.generations_without_snapshot
            != self.generations_included
        {
            return Err(invalid_response());
        }
        let scoped: usize = self
            .turns
            .by_scope
            .iter()
            .map(|scope| scope.turns_counted)
            .sum();
        if scoped + self.turns.turns_without_usage != self.turns.turns_counted {
            return Err(invalid_response());
        }
        let mut scopes = std::collections::BTreeSet::new();
        for scope in &self.turns.by_scope {
            if scope.scope.trim().is_empty() || !scopes.insert(scope.scope.as_str()) {
                return Err(invalid_response());
            }
            for missing in [
                scope.turns_missing.input_tokens,
                scope.turns_missing.output_tokens,
                scope.turns_missing.reasoning_tokens,
                scope.turns_missing.thinking_tokens,
                scope.turns_missing.cache_read_tokens,
                scope.turns_missing.cache_write_tokens,
                scope.turns_missing.total_tokens,
            ] {
                if missing > scope.turns_counted {
                    return Err(invalid_response());
                }
            }
        }
        for missing in [
            self.turns.turns_missing.input_tokens,
            self.turns.turns_missing.output_tokens,
            self.turns.turns_missing.reasoning_tokens,
            self.turns.turns_missing.thinking_tokens,
            self.turns.turns_missing.cache_read_tokens,
            self.turns.turns_missing.cache_write_tokens,
            self.turns.turns_missing.total_tokens,
        ] {
            if missing > self.turns.turns_counted {
                return Err(invalid_response());
            }
        }
        let mut units = std::collections::BTreeSet::new();
        for cost in &self.sessions.cost_by_unit {
            if cost.unit.trim().is_empty()
                || !(cost.total_amount.is_finite() && cost.total_amount >= 0.0)
                || cost.snapshots == 0
                || !units.insert(cost.unit.as_str())
            {
                return Err(invalid_response());
            }
        }
        Ok(())
    }
}

fn validate_text(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new(
            "invalid",
            "usage summary identifiers must be non-empty strings",
        ));
    }
    Ok(())
}

fn invalid_response() -> Error {
    Error::new("unknown", "usage summary response is invalid")
}

fn present_text<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(value) = &value {
        validate_text(value).map_err(serde::de::Error::custom)?;
    }
    Ok(value)
}

fn present_generation<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<i64>, D::Error> {
    let value = Option::<i64>::deserialize(deserializer)?;
    if value.is_some_and(|value| value < 0) {
        return Err(serde::de::Error::custom("generation must be nonnegative"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn exactly_one_scope_is_required() {
        for params in [
            json!({}),
            json!({"agent": "a1", "project": "p"}),
            json!({"agent": "a1", "root": "a2"}),
            json!({"project": "p", "root": "a2"}),
            json!({"generation": 1}),
            json!({"project": "p", "generation": 1}),
        ] {
            let request: UsageSummaryRequest = serde_json::from_value(params).unwrap();
            assert!(request.validate().is_err());
        }
        for params in [
            json!({"agent": "a1"}),
            json!({"agent": "a1", "generation": 2}),
            json!({"project": "p"}),
            json!({"root": "a2"}),
        ] {
            let request: UsageSummaryRequest = serde_json::from_value(params).unwrap();
            request.validate().unwrap();
        }
    }

    #[test]
    fn blank_and_malformed_identifiers_reject() {
        for params in [
            json!({"agent": ""}),
            json!({"agent": "  "}),
            json!({"agent": null}),
            json!({"project": ""}),
            json!({"root": "b1"}),
            json!({"agent": "a1", "generation": -1}),
            json!({"agent": "a1", "unknown": true}),
        ] {
            let decoded = serde_json::from_value::<UsageSummaryRequest>(params.clone());
            assert!(
                decoded.is_err() || decoded.unwrap().validate().is_err(),
                "must reject {params}",
            );
        }
        assert!(
            serde_json::from_value::<UsageSummaryRequest>(json!({"agent": "a0"}))
                .unwrap()
                .validate()
                .is_err()
        );
    }

    #[test]
    fn response_binding_checks_scope_and_counts() {
        let request: UsageSummaryRequest = serde_json::from_value(json!({"agent": "a1"})).unwrap();
        let mut reply = empty_response(&request);
        request.validate_response(&reply).unwrap();
        reply.scope.agent = Some("a2".to_owned());
        assert!(request.validate_response(&reply).is_err());
    }

    fn empty_response(request: &UsageSummaryRequest) -> UsageSummaryResponse {
        UsageSummaryResponse {
            scope: UsageScopeView {
                agent: request.agent.clone(),
                generation: request.generation,
                project: request.project.clone(),
                root: request.root.clone(),
            },
            agents_included: 1,
            generations_included: 1,
            truncated: false,
            history_complete: true,
            history_reason: "no event history pruned".to_owned(),
            pruned_through: 0,
            turns: TurnExpenditure {
                turns_counted: 0,
                turns_without_usage: 0,
                turns_missing: TurnCounterMissing::default(),
                by_scope: Vec::new(),
            },
            sessions: SessionFacts {
                snapshots: 0,
                generations_without_snapshot: 1,
                cumulative_tokens: SessionCumulativeTotals {
                    input_tokens: subtotal_none(),
                    output_tokens: subtotal_none(),
                    cache_read_tokens: subtotal_none(),
                    cache_write_tokens: subtotal_none(),
                },
                cost_by_unit: Vec::new(),
                snapshots_without_cost: 0,
                context_occupancy_supplied: 0,
                context_capacity_supplied: 0,
            },
        }
    }

    fn subtotal_none() -> SessionTokenSubtotal {
        SessionTokenSubtotal {
            total: None,
            supplied: 0,
            missing: 0,
            complete: false,
        }
    }
}
