use crate::{Error, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const AUTONOMY_DAY_MS: i64 = 24 * 60 * 60 * 1_000;

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AutonomyRequest {
    #[serde(default)]
    pub since_ms: Option<i64>,
}

impl AutonomyRequest {
    pub fn since(&self, now_ms: i64) -> Result<i64> {
        let since = self
            .since_ms
            .unwrap_or(now_ms.saturating_sub(AUTONOMY_DAY_MS));
        if !(0..=now_ms).contains(&since) {
            return Err(Error::invalid_field(
                "since_ms",
                "Unix milliseconds between 0 and now",
            ));
        }
        Ok(since)
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AutonomyResponse {
    pub since_ms: i64,
    pub until_ms: i64,
    pub project_policy: AutonomyRow<AutonomyProjectPolicy>,
    pub steer: AutonomyRow<AutonomySteer>,
    pub checks: AutonomyRow<AutonomyChecks>,
    pub waits: AutonomyRow<AutonomyWaits>,
    pub notes: AutonomyRow<AutonomyNotes>,
    pub owner_waiting: AutonomyRow<AutonomyOwnerWaiting>,
    pub usage: AutonomyRow<AutonomyUsage>,
    pub executor_swarm: AutonomyRow<AutonomyExecutorSwarm>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AutonomyRow<T> {
    pub source: String,
    pub coverage: AutonomyCoverage,
    pub reason: Option<String>,
    pub counts: Option<T>,
}

impl<T> AutonomyRow<T> {
    pub fn complete(source: &str, counts: T) -> Self {
        Self {
            source: source.into(),
            coverage: AutonomyCoverage::Complete,
            reason: None,
            counts: Some(counts),
        }
    }

    pub fn partial(source: &str, reason: &str, counts: T) -> Self {
        Self {
            source: source.into(),
            coverage: AutonomyCoverage::Partial,
            reason: Some(reason.into()),
            counts: Some(counts),
        }
    }

    pub fn unavailable(source: &str, reason: &str) -> Self {
        Self {
            source: source.into(),
            coverage: AutonomyCoverage::Unavailable,
            reason: Some(reason.into()),
            counts: None,
        }
    }

    pub fn limit_coverage(&mut self, reason: &str) {
        if self.coverage == AutonomyCoverage::Unavailable {
            return;
        }
        self.coverage = AutonomyCoverage::Partial;
        match &mut self.reason {
            Some(details) => {
                details.push_str(". ");
                details.push_str(reason);
            }
            None => self.reason = Some(reason.into()),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AutonomyCoverage {
    Complete,
    Partial,
    Unavailable,
}

impl AutonomyCoverage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Partial => "partial",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AutonomyProjectPolicy {
    pub created: u64,
    pub applied: u64,
    pub opted_out: u64,
    pub projects: Vec<AutonomyProject>,
    pub projects_truncated: bool,
    pub opt_outs: Vec<AutonomyOptOut>,
    pub opt_outs_truncated: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AutonomyProject {
    pub project: String,
    pub created: u64,
    pub applied: u64,
    pub opted_out: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AutonomyOptOut {
    pub agent: String,
    pub project: String,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AutonomySteer {
    pub manual: u64,
    pub swarm: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AutonomyChecks {
    pub manual: u64,
    pub swarm: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AutonomyWaits {
    pub calls: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AutonomyNotes {
    pub delivered: u64,
    #[serde(default)]
    pub moved: u64,
    #[serde(default)]
    pub cancelled: u64,
    #[serde(default)]
    pub expired: u64,
    #[serde(default)]
    pub by_kind: Vec<crate::ContextNoteCounters>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AutonomyOwnerWaiting {
    #[serde(default)]
    pub total: u64,
    #[serde(default)]
    pub by_kind: std::collections::BTreeMap<super::OwnerInboxKind, u64>,
    pub unknown_commands: u64,
    pub failed_or_lost_agents: u64,
    pub provider_holds: u64,
    pub oldest: Vec<AutonomyWaitingItem>,
    pub oldest_truncated: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AutonomyWaitingItem {
    pub kind: String,
    pub id: String,
    pub since_ms: i64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AutonomyUsage {
    pub manager_input_tokens: u64,
    pub executor_input_tokens: u64,
    pub observed_calls: u64,
    pub manager_input_share: Option<f64>,
    pub average_context_tokens: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AutonomyExecutorSwarm {
    pub profiles: Vec<AutonomyExecutorProfile>,
    pub profiles_truncated: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct AutonomyExecutorProfile {
    pub profile: String,
    pub executors: u64,
    pub called_swarm: u64,
}
