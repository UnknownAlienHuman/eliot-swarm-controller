//! Typed Eliot-mode Goal definitions, reads and explicit rounds.
//!
//! Native Goal mode parses but is refused with `unsupported`: the installed
//! harness plugins expose no Goal protocol. Cost limits are detected the same
//! way: no usage source exists yet.

use crate::{Error, Result, limits::MAX_TEXT_BYTES};
use serde::{Deserialize, Serialize};

/// Longest retained check-output snippet in a Goal view.
pub const MAX_GOAL_OBSERVATION_BYTES: usize = 4 * 1024;
/// Longest Goal objective text.
pub const MAX_GOAL_TEXT_BYTES: usize = 64 * 1024;
/// Longest single check argv element or cwd.
pub const MAX_GOAL_CHECK_ARG_BYTES: usize = 4 * 1024;
/// Most check argv elements.
pub const MAX_GOAL_CHECK_ARGS: usize = 32;
/// Check timeout bounds in milliseconds.
pub const MIN_GOAL_CHECK_TIMEOUT_MS: i64 = 1_000;
pub const MAX_GOAL_CHECK_TIMEOUT_MS: i64 = 600_000;
pub const DEFAULT_GOAL_CHECK_TIMEOUT_MS: i64 = 60_000;
/// Upper bound for every Goal turn/round/progress limit.
pub const MAX_GOAL_LIMIT: i64 = 1_000_000;
/// Cadence bounds in milliseconds. Below a minute a cadence would spin
/// checks; above a week the Goal outlives any useful unattended window.
pub const MIN_GOAL_CADENCE_MS: i64 = 60_000;
pub const MAX_GOAL_CADENCE_MS: i64 = 7 * 24 * 60 * 60 * 1000;
/// Reported while no usage source exists.
pub const GOAL_COST_UNSUPPORTED: &str = "unsupported (no usage source)";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalMode {
    Eliot,
    Native,
}

impl GoalMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eliot => "eliot",
            Self::Native => "native",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "eliot" => Ok(Self::Eliot),
            "native" => Ok(Self::Native),
            _ => Err(Error::new("invalid", "goal mode must be eliot or native")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalStatus {
    Active,
    Paused,
    Cleared,
    Completed,
    Blocked,
}

impl GoalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Cleared => "cleared",
            Self::Completed => "completed",
            Self::Blocked => "blocked",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "active" => Ok(Self::Active),
            "paused" => Ok(Self::Paused),
            "cleared" => Ok(Self::Cleared),
            "completed" => Ok(Self::Completed),
            "blocked" => Ok(Self::Blocked),
            _ => Err(Error::new("internal", "invalid stored goal status")),
        }
    }
}

/// Frozen Goal inputs. Any change advances the Goal revision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalDefinition {
    pub text: String,
    pub check: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_cwd: Option<String>,
    #[serde(default = "default_check_timeout")]
    pub check_timeout_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rounds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_without_progress: Option<i64>,
    /// Optional cadence in milliseconds. A cadenced Goal rounds on anchored
    /// time slots instead of idle events; without one rounds name idle seqs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cadence_ms: Option<i64>,
}

fn default_check_timeout() -> i64 {
    DEFAULT_GOAL_CHECK_TIMEOUT_MS
}

impl GoalDefinition {
    pub fn validate(&self) -> Result<()> {
        if self.text.trim().is_empty() || self.text.len() > MAX_GOAL_TEXT_BYTES {
            return Err(Error::new(
                "invalid",
                "goal text must be nonempty and at most 64 KiB",
            ));
        }
        if self.check.is_empty() || self.check.len() > MAX_GOAL_CHECK_ARGS {
            return Err(Error::new(
                "invalid",
                "goal check requires 1 to 32 explicit argv elements",
            ));
        }
        for arg in &self.check {
            if arg.is_empty() || arg.len() > MAX_GOAL_CHECK_ARG_BYTES {
                return Err(Error::new(
                    "invalid",
                    "goal check argv elements must be nonempty and at most 4 KiB",
                ));
            }
        }
        if let Some(cwd) = &self.check_cwd
            && (cwd.trim().is_empty() || cwd.len() > MAX_GOAL_CHECK_ARG_BYTES)
        {
            return Err(Error::new(
                "invalid",
                "goal check cwd must be nonempty and at most 4 KiB",
            ));
        }
        if !(MIN_GOAL_CHECK_TIMEOUT_MS..=MAX_GOAL_CHECK_TIMEOUT_MS).contains(&self.check_timeout_ms)
        {
            return Err(Error::new(
                "invalid",
                "goal check timeout must be between 1s and 10m",
            ));
        }
        for (name, limit) in [
            ("max_turns", self.max_turns),
            ("max_rounds", self.max_rounds),
            ("max_without_progress", self.max_without_progress),
        ] {
            if let Some(limit) = limit
                && !(1..=MAX_GOAL_LIMIT).contains(&limit)
            {
                return Err(Error::new(
                    "invalid",
                    format!("goal {name} must be between 1 and 1000000"),
                ));
            }
        }
        if let Some(cadence_ms) = self.cadence_ms
            && !(MIN_GOAL_CADENCE_MS..=MAX_GOAL_CADENCE_MS).contains(&cadence_ms)
        {
            return Err(Error::new(
                "invalid",
                "goal cadence must be between 1 minute and 7 days in milliseconds",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalCounters {
    #[serde(default)]
    pub rounds: i64,
    #[serde(default)]
    pub turns_at_set: i64,
    #[serde(default)]
    pub turns_used: i64,
    #[serde(default)]
    pub consecutive_same_output: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_output_digest: Option<String>,
    /// Digest of the last complete read-only workspace fingerprint. Absent
    /// when the Goal has no Git observation or the latest one is incomplete;
    /// an incomplete fingerprint never extends a no-progress streak.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_observation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_check_at: Option<i64>,
}

impl GoalCounters {
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("rounds", self.rounds),
            ("turns_at_set", self.turns_at_set),
            ("turns_used", self.turns_used),
            ("consecutive_same_output", self.consecutive_same_output),
        ] {
            if value < 0 {
                return Err(Error::new(
                    "invalid",
                    format!("goal counter {name} must be nonnegative"),
                ));
            }
        }
        if let Some(observation) = &self.last_observation
            && observation.len() > MAX_GOAL_OBSERVATION_BYTES
        {
            return Err(Error::new("invalid", "goal observation exceeds 4 KiB"));
        }
        Ok(())
    }
}

/// Durable single-flight marker for one round: `running` while its check
/// runs, `unknown` when the round ended without a commit (check unknown,
/// interference, crash). An unknown marker burns its round key: the next
/// round needs a new idle key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalMarkerState {
    Running,
    Unknown,
}

impl GoalMarkerState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "running" => Ok(Self::Running),
            "unknown" => Ok(Self::Unknown),
            _ => Err(Error::new("internal", "invalid stored goal marker state")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalRoundMarker {
    pub round_key: String,
    pub generation: i64,
    pub revision: i64,
    pub state: GoalMarkerState,
    pub updated_at: i64,
}

impl GoalRoundMarker {
    pub fn validate(&self) -> Result<()> {
        if self.round_key.trim().is_empty() {
            return Err(Error::new(
                "invalid",
                "goal round marker key must be nonempty",
            ));
        }
        if self.generation < 0 {
            return Err(Error::new(
                "invalid",
                "goal round marker generation must be nonnegative",
            ));
        }
        if self.revision < 0 {
            return Err(Error::new(
                "invalid",
                "goal round marker revision must be nonnegative",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalView {
    pub agent: String,
    pub mode: GoalMode,
    pub generation: i64,
    pub revision: i64,
    /// Epoch-millisecond anchor of a cadenced Goal's slots, captured when
    /// the cadence was set and stable across pause/resume. Absent without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cadence_anchor_ms: Option<i64>,
    pub status: GoalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    pub definition: GoalDefinition,
    pub counters: GoalCounters,
    pub cost_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_round_key: Option<String>,
    /// Live in-flight round if any, else the latest unknown marker of this
    /// generation. Absent when no round is unresolved.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round_marker: Option<GoalRoundMarker>,
    pub updated_at: i64,
}

impl GoalView {
    pub fn validate(&self) -> Result<()> {
        validate_agent(&self.agent)?;
        if self.generation < 0 {
            return Err(Error::new("invalid", "goal generation must be nonnegative"));
        }
        if self.revision < 0 {
            return Err(Error::new("invalid", "goal revision must be nonnegative"));
        }
        if self.cadence_anchor_ms.is_some_and(|anchor| anchor < 0) {
            return Err(Error::new(
                "invalid",
                "goal cadence anchor must be nonnegative",
            ));
        }
        if self.definition.cadence_ms.is_none() && self.cadence_anchor_ms.is_some() {
            return Err(Error::new("invalid", "goal cadence anchor needs a cadence"));
        }
        if self.definition.cadence_ms.is_some() && self.cadence_anchor_ms.is_none() {
            return Err(Error::new(
                "invalid",
                "cadenced goal needs its captured anchor",
            ));
        }
        self.definition.validate()?;
        self.counters.validate()?;
        if self.cost_status != GOAL_COST_UNSUPPORTED {
            return Err(Error::new("invalid", "goal cost status is not recognized"));
        }
        if let Some(marker) = &self.round_marker {
            marker.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalSetRequest {
    pub agent: String,
    #[serde(default = "default_mode")]
    pub mode: GoalMode,
    pub definition: GoalDefinition,
    /// Detected and refused: no usage source exists yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost: Option<f64>,
}

fn default_mode() -> GoalMode {
    GoalMode::Eliot
}

impl GoalSetRequest {
    pub fn validate(&self) -> Result<()> {
        validate_agent(&self.agent)?;
        if self.max_cost.is_some() {
            return Err(Error::new(
                "unsupported",
                "goal cost limits need a usage source; usage is unsupported",
            ));
        }
        self.definition.validate()
    }

    pub fn validate_response(&self, response: &GoalView) -> Result<()> {
        if self.validate().is_err()
            || response.agent != self.agent
            || response.mode != self.mode
            || response.definition != self.definition
            || response.validate().is_err()
        {
            return Err(Error::new(
                "unknown",
                "goal.set response does not match the requested goal",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalGetRequest {
    pub agent: String,
    #[serde(default = "default_mode")]
    pub mode: GoalMode,
}

impl GoalGetRequest {
    pub fn validate(&self) -> Result<()> {
        validate_agent(&self.agent)
    }

    pub fn validate_response(&self, response: &GoalView) -> Result<()> {
        if self.validate().is_err()
            || response.agent != self.agent
            || response.mode != self.mode
            || response.validate().is_err()
        {
            return Err(Error::new(
                "unknown",
                "goal.get response does not match the requested goal",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalPauseRequest {
    pub agent: String,
    #[serde(default = "default_mode")]
    pub mode: GoalMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl GoalPauseRequest {
    pub fn validate(&self) -> Result<()> {
        validate_agent(&self.agent)?;
        if let Some(reason) = &self.reason
            && (reason.trim().is_empty() || reason.len() > MAX_GOAL_OBSERVATION_BYTES)
        {
            return Err(Error::new(
                "invalid",
                "goal pause reason must be nonempty and at most 4 KiB",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &GoalView) -> Result<()> {
        if self.validate().is_err()
            || response.agent != self.agent
            || response.mode != self.mode
            || response.validate().is_err()
        {
            return Err(Error::new(
                "unknown",
                "goal.pause response does not match the requested goal",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalClearRequest {
    pub agent: String,
    #[serde(default = "default_mode")]
    pub mode: GoalMode,
}

impl GoalClearRequest {
    pub fn validate(&self) -> Result<()> {
        validate_agent(&self.agent)
    }

    pub fn validate_response(&self, response: &GoalView) -> Result<()> {
        if self.validate().is_err()
            || response.agent != self.agent
            || response.mode != self.mode
            || response.validate().is_err()
        {
            return Err(Error::new(
                "unknown",
                "goal.clear response does not match the requested goal",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalResumeRequest {
    pub agent: String,
    #[serde(default = "default_mode")]
    pub mode: GoalMode,
}

impl GoalResumeRequest {
    pub fn validate(&self) -> Result<()> {
        validate_agent(&self.agent)
    }

    pub fn validate_response(&self, response: &GoalView) -> Result<()> {
        if self.validate().is_err()
            || response.agent != self.agent
            || response.mode != self.mode
            || response.validate().is_err()
        {
            return Err(Error::new(
                "unknown",
                "goal.resume response does not match the requested goal",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalRoundRequest {
    pub id: String,
    pub agent: String,
    #[serde(default = "default_mode")]
    pub mode: GoalMode,
    /// Idle-event key. Exactly one of the two key forms is present; the
    /// form must match the Goal (slots for cadenced Goals, idle seqs else).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_seq: Option<i64>,
    /// Anchored cadence slot index (first slot is 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot: Option<i64>,
}

impl GoalRoundRequest {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() || self.id.len() > MAX_TEXT_BYTES {
            return Err(Error::new("invalid", "goal round id must be nonempty"));
        }
        validate_agent(&self.agent)?;
        match (self.idle_seq, self.slot) {
            (Some(idle_seq), None) => {
                if idle_seq < 0 {
                    return Err(Error::new(
                        "invalid",
                        "goal round idle_seq must be nonnegative",
                    ));
                }
            }
            (None, Some(slot)) => {
                if slot < 1 {
                    return Err(Error::new("invalid", "goal round slot must be at least 1"));
                }
            }
            _ => {
                return Err(Error::new(
                    "invalid",
                    "goal round needs exactly one of idle_seq, slot",
                ));
            }
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &GoalRoundResponse) -> Result<()> {
        if self.validate().is_err()
            || response.view.agent != self.agent
            || response.view.mode != self.mode
            || response.view.validate().is_err()
        {
            return Err(Error::new(
                "unknown",
                "goal.round response does not match the requested goal",
            ));
        }
        match response.outcome {
            GoalRoundOutcome::Continued => {
                if response.command_id.as_deref() != Some(self.id.as_str()) {
                    return Err(Error::new(
                        "unknown",
                        "goal.round continuation does not match the requested mutation",
                    ));
                }
                let Some(continuation) = &response.continuation else {
                    return Err(Error::new(
                        "unknown",
                        "goal.round continuation omits its admitted command",
                    ));
                };
                if continuation.command_id != self.id {
                    return Err(Error::new(
                        "unknown",
                        "goal.round continuation does not match the requested mutation",
                    ));
                }
                if response.reason.is_some() {
                    return Err(Error::new(
                        "unknown",
                        "continued goal.round response must not carry a block reason",
                    ));
                }
            }
            GoalRoundOutcome::Blocked => {
                if response
                    .reason
                    .as_ref()
                    .is_none_or(|reason| reason.trim().is_empty())
                    || response.command_id.is_some()
                    || response.continuation.is_some()
                {
                    return Err(Error::new(
                        "unknown",
                        "blocked goal.round response must carry only a reason",
                    ));
                }
            }
            GoalRoundOutcome::Completed => {
                if response.reason.is_some()
                    || response.command_id.is_some()
                    || response.continuation.is_some()
                {
                    return Err(Error::new(
                        "unknown",
                        "completed goal.round response must not carry a continuation",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalRoundOutcome {
    Completed,
    Blocked,
    Continued,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalRoundResponse {
    pub outcome: GoalRoundOutcome,
    /// Blocked only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Continued only; always the requested mutation id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
    pub view: GoalView,
    /// Continued only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<crate::api::CommandSubmitResponse>,
}

fn validate_agent(agent: &str) -> Result<()> {
    if agent.trim().is_empty() {
        return Err(Error::new("invalid", "goal agent must be nonempty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn definition() -> GoalDefinition {
        GoalDefinition {
            text: "keep the tree green".to_owned(),
            check: vec!["check-goal".to_owned(), "--strict".to_owned()],
            check_cwd: None,
            check_timeout_ms: DEFAULT_GOAL_CHECK_TIMEOUT_MS,
            max_turns: Some(5),
            max_rounds: Some(10),
            max_without_progress: Some(3),
            cadence_ms: None,
        }
    }

    fn view() -> GoalView {
        GoalView {
            agent: "a1".to_owned(),
            mode: GoalMode::Eliot,
            generation: 2,
            revision: 1,
            cadence_anchor_ms: None,
            status: GoalStatus::Active,
            paused_reason: None,
            blocked_reason: None,
            definition: definition(),
            counters: GoalCounters::default(),
            cost_status: GOAL_COST_UNSUPPORTED.to_owned(),
            last_round_key: None,
            round_marker: None,
            updated_at: 7,
        }
    }

    #[test]
    fn goal_modes_parse_and_round_trip() {
        assert_eq!(GoalMode::parse("eliot").unwrap(), GoalMode::Eliot);
        assert_eq!(GoalMode::parse("native").unwrap(), GoalMode::Native);
        assert!(GoalMode::parse("codex").is_err());
        assert_eq!(
            serde_json::to_value(GoalMode::Native).unwrap(),
            json!("native")
        );
        for status in [
            GoalStatus::Active,
            GoalStatus::Paused,
            GoalStatus::Cleared,
            GoalStatus::Completed,
            GoalStatus::Blocked,
        ] {
            assert_eq!(GoalStatus::parse(status.as_str()).unwrap(), status);
            assert_eq!(
                serde_json::from_value::<GoalStatus>(serde_json::to_value(status).unwrap())
                    .unwrap(),
                status
            );
        }
        assert!(GoalStatus::parse("driving").is_err());
    }

    #[test]
    fn goal_definitions_validate_bounds_and_limits() {
        definition().validate().unwrap();
        let mut blank = definition();
        blank.text = "  ".to_owned();
        assert!(blank.validate().is_err());
        let mut no_check = definition();
        no_check.check.clear();
        assert!(no_check.validate().is_err());
        let mut blank_arg = definition();
        blank_arg.check[0] = String::new();
        assert!(blank_arg.validate().is_err());
        let mut bad_timeout = definition();
        bad_timeout.check_timeout_ms = 999;
        assert!(bad_timeout.validate().is_err());
        bad_timeout.check_timeout_ms = 600_001;
        assert!(bad_timeout.validate().is_err());
        let mut bad_limit = definition();
        bad_limit.max_turns = Some(0);
        assert!(bad_limit.validate().is_err());
        bad_limit.max_turns = Some(MAX_GOAL_LIMIT + 1);
        assert!(bad_limit.validate().is_err());
        let mut omitted = definition();
        omitted.max_turns = None;
        omitted.max_rounds = None;
        omitted.max_without_progress = None;
        omitted.validate().unwrap();
        let mut cadenced = definition();
        cadenced.cadence_ms = Some(MIN_GOAL_CADENCE_MS);
        cadenced.validate().unwrap();
        cadenced.cadence_ms = Some(MAX_GOAL_CADENCE_MS);
        cadenced.validate().unwrap();
        for bad in [0, MIN_GOAL_CADENCE_MS - 1, MAX_GOAL_CADENCE_MS + 1] {
            cadenced.cadence_ms = Some(bad);
            assert!(cadenced.validate().is_err());
        }
        let wire = serde_json::to_value(&omitted).unwrap();
        assert_eq!(
            serde_json::from_value::<GoalDefinition>(wire).unwrap(),
            omitted
        );
        let defaulted: GoalDefinition =
            serde_json::from_value(json!({"text":"t","check":["c"]})).unwrap();
        assert_eq!(defaulted.check_timeout_ms, DEFAULT_GOAL_CHECK_TIMEOUT_MS);
        defaulted.validate().unwrap();
    }

    #[test]
    fn goal_set_detects_cost_limits_and_binds_its_response() {
        let request = GoalSetRequest {
            agent: "a1".to_owned(),
            mode: GoalMode::Eliot,
            definition: definition(),
            max_cost: None,
        };
        request.validate().unwrap();
        request.validate_response(&view()).unwrap();
        let with_cost = GoalSetRequest {
            max_cost: Some(10.0),
            ..request.clone()
        };
        assert_eq!(with_cost.validate().unwrap_err().code, "unsupported");
        let mut wrong_definition = view();
        wrong_definition.definition.text = "other".to_owned();
        assert!(request.validate_response(&wrong_definition).is_err());
        for invalid in [
            json!({"agent":"a1"}),
            json!({"agent":"a1","mode":"codex","definition":definition()}),
            json!({"agent":"a1","definition":definition(),"extra":true}),
        ] {
            assert!(serde_json::from_value::<GoalSetRequest>(invalid).is_err());
        }
        let blank: GoalSetRequest =
            serde_json::from_value(json!({"agent":" ","definition":definition()})).unwrap();
        assert!(blank.validate().is_err());
        let defaulted: GoalSetRequest =
            serde_json::from_value(json!({"agent":"a1","definition":definition()})).unwrap();
        assert_eq!(defaulted.mode, GoalMode::Eliot);
    }

    #[test]
    fn goal_reads_and_transitions_bind_agent_and_mode() {
        view().validate().unwrap();
        let mut bad_cost = view();
        bad_cost.cost_status = "tracked".to_owned();
        assert!(bad_cost.validate().is_err());
        GoalGetRequest {
            agent: "a1".to_owned(),
            mode: GoalMode::Eliot,
        }
        .validate_response(&view())
        .unwrap();
        let mut wrong_mode = view();
        wrong_mode.mode = GoalMode::Native;
        assert!(
            GoalGetRequest {
                agent: "a1".to_owned(),
                mode: GoalMode::Eliot,
            }
            .validate_response(&wrong_mode)
            .is_err()
        );
        let pause = GoalPauseRequest {
            agent: "a1".to_owned(),
            mode: GoalMode::Eliot,
            reason: Some("waiting".to_owned()),
        };
        pause.validate().unwrap();
        pause.validate_response(&view()).unwrap();
        let blank_reason = GoalPauseRequest {
            reason: Some(" ".to_owned()),
            ..pause.clone()
        };
        assert!(blank_reason.validate().is_err());
        GoalClearRequest {
            agent: "a1".to_owned(),
            mode: GoalMode::Eliot,
        }
        .validate_response(&view())
        .unwrap();
        GoalResumeRequest {
            agent: "a1".to_owned(),
            mode: GoalMode::Eliot,
        }
        .validate_response(&view())
        .unwrap();
    }

    #[test]
    fn goal_round_markers_validate_and_round_trip() {
        assert_eq!(
            GoalMarkerState::parse("running").unwrap(),
            GoalMarkerState::Running
        );
        assert_eq!(
            GoalMarkerState::parse("unknown").unwrap(),
            GoalMarkerState::Unknown
        );
        assert!(GoalMarkerState::parse("driving").is_err());
        let marker = GoalRoundMarker {
            round_key: "idle:9".to_owned(),
            generation: 2,
            revision: 1,
            state: GoalMarkerState::Unknown,
            updated_at: 7,
        };
        marker.validate().unwrap();
        let wire = serde_json::to_value(&marker).unwrap();
        assert_eq!(
            serde_json::from_value::<GoalRoundMarker>(wire).unwrap(),
            marker
        );
        let mut marked = view();
        marked.round_marker = Some(marker);
        marked.validate().unwrap();
        let mut blank = marked.clone();
        blank.round_marker.as_mut().unwrap().round_key = " ".to_owned();
        assert!(blank.validate().is_err());
        // Old daemons omit the marker; it defaults to absent.
        let unmarked: GoalView = serde_json::from_value(json!({
            "agent": "a1", "mode": "eliot", "generation": 2, "revision": 1,
            "status": "active", "definition": definition(),
            "counters": GoalCounters::default(),
            "cost_status": GOAL_COST_UNSUPPORTED, "updated_at": 7,
        }))
        .unwrap();
        assert!(unmarked.round_marker.is_none());
    }

    #[test]
    fn goal_round_binds_the_mutation_only_for_continuations() {
        let request = GoalRoundRequest {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            agent: "a1".to_owned(),
            mode: GoalMode::Eliot,
            idle_seq: Some(9),
            slot: None,
        };
        request.validate().unwrap();
        let completed = GoalRoundResponse {
            outcome: GoalRoundOutcome::Completed,
            reason: None,
            command_id: None,
            view: view(),
            continuation: None,
        };
        request.validate_response(&completed).unwrap();
        let wire = serde_json::to_value(&completed).unwrap();
        assert_eq!(wire["outcome"], json!("completed"));
        assert_eq!(
            serde_json::from_value::<GoalRoundResponse>(wire).unwrap(),
            completed
        );
        let blocked = GoalRoundResponse {
            outcome: GoalRoundOutcome::Blocked,
            reason: Some("turn_limit".to_owned()),
            command_id: None,
            view: view(),
            continuation: None,
        };
        request.validate_response(&blocked).unwrap();
        let wire = serde_json::to_value(&blocked).unwrap();
        assert_eq!(wire["reason"], json!("turn_limit"));
        assert_eq!(
            serde_json::from_value::<GoalRoundResponse>(wire).unwrap(),
            blocked
        );
        let continuation = crate::api::CommandSubmitResponse {
            command_id: request.id.clone(),
            agent: Some("a1".to_owned()),
            generation: Some(2),
            state: crate::CommandState::Accepted,
            outcome: None,
            linked_command: None,
        };
        let continued = GoalRoundResponse {
            outcome: GoalRoundOutcome::Continued,
            reason: None,
            command_id: Some(request.id.clone()),
            view: view(),
            continuation: Some(continuation),
        };
        request.validate_response(&continued).unwrap();
        let wire = serde_json::to_value(&continued).unwrap();
        assert_eq!(
            serde_json::from_value::<GoalRoundResponse>(wire).unwrap(),
            continued
        );
        let mismatched = GoalRoundResponse {
            outcome: GoalRoundOutcome::Continued,
            reason: None,
            command_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned()),
            view: view(),
            continuation: None,
        };
        assert!(request.validate_response(&mismatched).is_err());
        let terminal_with_command = GoalRoundResponse {
            continuation: continued.continuation.clone(),
            ..completed.clone()
        };
        assert!(request.validate_response(&terminal_with_command).is_err());
        let blocked_without_reason = GoalRoundResponse {
            reason: None,
            ..blocked.clone()
        };
        assert!(request.validate_response(&blocked_without_reason).is_err());
        for invalid in [
            json!({"agent":"a1","idle_seq":9}),
            json!({"id":"x","agent":"a1","idle_seq":9,"mode":"codex"}),
        ] {
            assert!(serde_json::from_value::<GoalRoundRequest>(invalid).is_err());
        }
        for invalid in [
            json!({"id":" ","agent":"a1","idle_seq":9}),
            json!({"id":"x","agent":"a1","idle_seq":-1}),
        ] {
            let request: GoalRoundRequest = serde_json::from_value(invalid).unwrap();
            assert!(request.validate().is_err());
        }
    }

    #[test]
    fn goal_round_takes_exactly_one_key_form() {
        let slot = GoalRoundRequest {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            agent: "a1".to_owned(),
            mode: GoalMode::Eliot,
            idle_seq: None,
            slot: Some(3),
        };
        slot.validate().unwrap();
        let wire = serde_json::to_value(&slot).unwrap();
        assert_eq!(wire["slot"], json!(3));
        assert!(wire.get("idle_seq").is_none());
        assert_eq!(
            serde_json::from_value::<GoalRoundRequest>(wire).unwrap(),
            slot
        );
        // Old idle-only requests still parse.
        let legacy: GoalRoundRequest =
            serde_json::from_value(json!({"id":"x","agent":"a1","idle_seq":9})).unwrap();
        assert_eq!((legacy.idle_seq, legacy.slot), (Some(9), None));
        legacy.validate().unwrap();
        for invalid in [
            json!({"id":"x","agent":"a1"}),
            json!({"id":"x","agent":"a1","idle_seq":9,"slot":3}),
            json!({"id":"x","agent":"a1","slot":0}),
            json!({"id":"x","agent":"a1","slot":-2}),
        ] {
            let request: GoalRoundRequest = serde_json::from_value(invalid).unwrap();
            assert!(request.validate().is_err());
        }
    }

    #[test]
    fn goal_cadence_anchor_binds_to_its_cadence() {
        let mut cadenced = view();
        cadenced.definition.cadence_ms = Some(MIN_GOAL_CADENCE_MS);
        assert!(cadenced.validate().is_err());
        cadenced.cadence_anchor_ms = Some(1_700_000_000_000);
        cadenced.validate().unwrap();
        let wire = serde_json::to_value(&cadenced).unwrap();
        assert_eq!(wire["cadence_anchor_ms"], json!(1_700_000_000_000i64));
        assert_eq!(serde_json::from_value::<GoalView>(wire).unwrap(), cadenced);
        let mut anchored = view();
        anchored.cadence_anchor_ms = Some(7);
        assert!(anchored.validate().is_err());
        let mut negative = cadenced.clone();
        negative.cadence_anchor_ms = Some(-1);
        assert!(negative.validate().is_err());
        // Old views omit the anchor; it defaults to absent.
        let unanchored: GoalView = serde_json::from_value(json!({
            "agent": "a1", "mode": "eliot", "generation": 2, "revision": 1,
            "status": "active", "definition": definition(),
            "counters": GoalCounters::default(),
            "cost_status": GOAL_COST_UNSUPPORTED, "updated_at": 7,
        }))
        .unwrap();
        assert!(unanchored.cadence_anchor_ms.is_none());
        unanchored.validate().unwrap();
    }
}
