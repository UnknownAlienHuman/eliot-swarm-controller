//! Typed Eliot-mode Goal definitions, reads and explicit rounds.
//!
//! Native Goal mode parses but is refused with `unsupported`: the installed
//! harness plugins expose no Goal protocol. A money budget (`max_cost`) is
//! kept only from the Agent's native session cost in its own unit.

use crate::{Error, Result, api::coordination::normalize_claim_glob, limits::MAX_TEXT_BYTES};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
/// Largest Goal money budget, in millionths of its unit.
pub const MAX_GOAL_COST_MICROS: i64 = 1_000_000_000_000_000;
/// Longest native cost unit name.
const MAX_GOAL_COST_UNIT_BYTES: usize = 64;
/// Most `scope` or `check_inputs` globs of one Goal.
pub const MAX_GOAL_SCOPE_GLOBS: usize = 32;
/// Most paths already changed when a scoped Goal is set; a larger starting
/// change set is refused rather than recorded incompletely.
pub const MAX_GOAL_SCOPE_BASE_PATHS: usize = 512;

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

/// Goal-owned work for a folder assignment. The brief's Agent keeps working
/// under this check until its Goal completes or blocks, and only then
/// retires. The check runs in the Agent's own workspace.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignmentGoal {
    pub check: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_timeout_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rounds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_without_progress: Option<i64>,
}

impl AssignmentGoal {
    /// The Goal frozen for one selected brief.
    pub fn definition(&self, brief: &str) -> GoalDefinition {
        GoalDefinition {
            text: format!(
                "Finish the assigned brief {brief}. The Goal check decides when it is done."
            ),
            check: self.check.clone(),
            check_cwd: None,
            check_timeout_ms: self
                .check_timeout_ms
                .unwrap_or(DEFAULT_GOAL_CHECK_TIMEOUT_MS),
            max_turns: self.max_turns,
            max_rounds: self.max_rounds,
            max_without_progress: self.max_without_progress,
            cadence_ms: None,
            scope: Vec::new(),
            check_inputs: Vec::new(),
            max_cost: None,
            escalation: None,
        }
    }

    /// Assignment Goals drive automatically on idle, so they need a turn or
    /// round budget like any cadence-less Goal.
    pub fn validate(&self) -> Result<()> {
        let definition = self.definition("brief.md");
        definition.validate()?;
        definition.require_auto_bound()
    }
}

/// Frozen Goal inputs. Any change advances the Goal revision.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Paths the Goal's work may change: project-relative globs (`/`, `*`,
    /// `**`, `?`) under the workspace's Git root. With it Swarm records the
    /// starting point at `set` and blocks the Goal on any change outside.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scope: Vec<String>,
    /// The check's own inputs, as the same globs. A passing check after one
    /// of them changed is not accepted as completion: the Goal blocks for a
    /// separate review. Workspace files named by the check argv are added.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub check_inputs: Vec<String>,
    /// Money budget over the Agent's native session cost since `set`, in
    /// that native unit (never converted). Checked after each failed check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost: Option<GoalCostLimit>,
    /// One Owner-authorized model or effort change, applied once when the
    /// same failure repeats after the diagnosis step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation: Option<GoalEscalation>,
}

/// A Goal's one escalation: a `configure` of model and/or effort with the
/// Owner's stated reason. Never a ladder and never wider rights.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalEscalation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    pub reason: String,
}

impl GoalEscalation {
    fn validate(&self) -> Result<()> {
        if self.model.is_none() && self.effort.is_none() {
            return Err(Error::new(
                "invalid",
                "goal escalation needs a model or an effort",
            ));
        }
        for value in [&self.model, &self.effort].into_iter().flatten() {
            if value.trim().is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
                return Err(Error::new(
                    "invalid",
                    "goal escalation model and effort must be short names",
                ));
            }
        }
        if self.reason.trim().is_empty()
            || self.reason.len() > 256
            || self.reason.chars().any(char::is_control)
        {
            return Err(Error::new(
                "invalid",
                "goal escalation needs a one-line reason of at most 256 bytes",
            ));
        }
        Ok(())
    }

    /// The `configure` args this escalation submits.
    pub fn configure_args(&self) -> serde_json::Value {
        let mut args = serde_json::Map::new();
        if let Some(model) = &self.model {
            args.insert("model".to_owned(), model.clone().into());
        }
        if let Some(effort) = &self.effort {
            args.insert("effort".to_owned(), effort.clone().into());
        }
        serde_json::Value::Object(args)
    }
}

/// What a failed round's continuation asks of the Agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GoalContinuationStage {
    /// Fix what the check reports.
    Repair,
    /// Two approaches failed the same way: diagnose before changing more.
    Diagnose,
    /// The same failure followed the diagnosis: the one escalation applies.
    Escalate,
}

/// A Goal's money budget: millionths of one verbatim native cost unit, such
/// as Claude's `usd_estimate`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalCostLimit {
    pub unit: String,
    pub amount_micros: i64,
}

impl GoalCostLimit {
    fn validate(&self) -> Result<()> {
        if self.unit.is_empty()
            || self.unit.len() > MAX_GOAL_COST_UNIT_BYTES
            || self
                .unit
                .chars()
                .any(|c| c.is_control() || c.is_whitespace())
        {
            return Err(Error::new(
                "invalid",
                "goal cost unit must be a nonempty native unit name of at most 64 bytes",
            ));
        }
        if !(1..=MAX_GOAL_COST_MICROS).contains(&self.amount_micros) {
            return Err(Error::new(
                "invalid",
                "goal cost limit must be positive and at most 1e9 units",
            ));
        }
        Ok(())
    }
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
        if let Some(limit) = &self.max_cost {
            limit.validate()?;
        }
        if let Some(escalation) = &self.escalation {
            escalation.validate()?;
        }
        for (name, globs) in [("scope", &self.scope), ("check_inputs", &self.check_inputs)] {
            if globs.len() > MAX_GOAL_SCOPE_GLOBS {
                return Err(Error::new(
                    "invalid",
                    format!("goal {name} takes at most {MAX_GOAL_SCOPE_GLOBS} globs"),
                ));
            }
            for glob in globs {
                normalize_claim_glob(glob).map_err(|_| {
                    Error::new(
                        "invalid",
                        format!(
                            "goal {name} globs are project-relative with / separators and *, ** and ? only"
                        ),
                    )
                })?;
            }
        }
        Ok(())
    }

    /// Whether Swarm records a starting point and checks the change scope.
    pub fn tracks_changes(&self) -> bool {
        !self.scope.is_empty() || !self.check_inputs.is_empty()
    }

    /// Automatic `on_idle` driving needs a bound: a Goal without a cadence
    /// is driven on every idle event, so it must set `max_turns` or
    /// `max_rounds`. Cadenced Goals round only on explicit slots and need
    /// none. Checked at `set`, never on stored views.
    pub fn require_auto_bound(&self) -> Result<()> {
        if self.cadence_ms.is_none() && self.max_turns.is_none() && self.max_rounds.is_none() {
            return Err(Error::new(
                "invalid",
                "goal without a cadence drives automatically on idle and needs max_turns or max_rounds",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Workspace fingerprint read right after the last failed check; the next
    /// round compares its pre-check read with it, so the check's own writes
    /// are never progress. Absent without a Git binding or after a failed read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_observation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_check_at: Option<i64>,
    /// Session cost spent since `set` in the `max_cost` unit, in millionths,
    /// as of the last round. Absent without a budget or while unverified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_spent_micros: Option<i64>,
    /// Why the spent cost could not be verified at the last round.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_unverified: Option<String>,
    /// Consecutive failed rounds whose fully captured check output repeated
    /// the previous one, whatever the workspace did.
    #[serde(default)]
    pub consecutive_same_failure: i64,
    /// Round whose continuation was the diagnosis step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnosis_round: Option<i64>,
    /// Round whose continuation followed the one escalation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation_round: Option<i64>,
}

impl GoalCounters {
    /// The same-failure streak after a failed check with this output, and
    /// the continuation it calls for: the second identical failure in a row
    /// (two failed approaches) asks for one diagnosis, and an identical
    /// failure after that applies the Goal's one escalation, if any.
    pub fn failure_stage(
        &self,
        definition: &GoalDefinition,
        output_complete: bool,
        output_digest: Option<&str>,
    ) -> (i64, GoalContinuationStage) {
        let same = output_complete
            && output_digest.is_some()
            && self.last_output_digest.as_deref() == output_digest;
        let streak = if same {
            self.consecutive_same_failure.saturating_add(1)
        } else {
            0
        };
        let stage = if streak >= 2
            && self.diagnosis_round.is_some()
            && self.escalation_round.is_none()
            && definition.escalation.is_some()
        {
            GoalContinuationStage::Escalate
        } else if streak >= 1 && self.diagnosis_round.is_none() {
            GoalContinuationStage::Diagnose
        } else {
            GoalContinuationStage::Repair
        };
        (streak, stage)
    }

    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("rounds", self.rounds),
            ("turns_at_set", self.turns_at_set),
            ("turns_used", self.turns_used),
            ("consecutive_same_output", self.consecutive_same_output),
            ("consecutive_same_failure", self.consecutive_same_failure),
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
        if self.cost_spent_micros.is_some_and(|spent| spent < 0) {
            return Err(Error::new("invalid", "goal spent cost must be nonnegative"));
        }
        Ok(())
    }
}

/// Starting point Swarm records at `set` for a Goal with `scope` or
/// `check_inputs`. Paths are relative to the Git root `root`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalScopeBase {
    pub root: String,
    /// HEAD at set; absent on an unborn branch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    /// Content digest of each path that already differed from HEAD at set.
    /// That content is the starting point, not the Goal's work.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub changed: BTreeMap<String, String>,
    /// Workspace files named by the check argv; they count as check inputs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub check_files: Vec<String>,
}

impl GoalScopeBase {
    pub fn start(&self) -> GoalScopeStart {
        GoalScopeStart {
            head: self.head.clone(),
            changed_paths: self.changed.len(),
            check_files: self.check_files.clone(),
        }
    }
}

/// What a Goal view shows of its recorded starting point, so a manager can
/// compare the final change with it.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalScopeStart {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    /// Paths already changed at set (their content then is not the Goal's work).
    pub changed_paths: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub check_files: Vec<String>,
}

/// Durable single-flight marker for one round: `running` while its check
/// runs, `unknown` when the round ended without a commit (check unknown,
/// interference, crash). An unknown marker burns its round key: the next
/// round needs a new idle key.
#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Starting point of a Goal with `scope` or `check_inputs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_start: Option<GoalScopeStart>,
    /// `untracked` without `max_cost`; otherwise the spent and budgeted
    /// amounts, or why the spent cost is unverified.
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
        if self.cost_status.trim().is_empty() || self.cost_status.len() > 256 {
            return Err(Error::new("invalid", "goal cost status must be short text"));
        }
        if let Some(marker) = &self.round_marker {
            marker.validate()?;
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalSetRequest {
    pub id: String,
    pub agent: String,
    #[serde(default = "default_mode")]
    pub mode: GoalMode,
    pub definition: GoalDefinition,
}

fn default_mode() -> GoalMode {
    GoalMode::Eliot
}

impl GoalSetRequest {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() || self.id.len() > MAX_TEXT_BYTES {
            return Err(Error::new("invalid", "goal set id must be nonempty"));
        }
        validate_agent(&self.agent)?;
        self.definition.validate()?;
        // Native mode is refused as unsupported either way; only Eliot
        // Goals drive automatically and need the bound.
        if self.mode == GoalMode::Eliot {
            self.definition.require_auto_bound()?;
        }
        Ok(())
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalPauseRequest {
    pub id: String,
    pub agent: String,
    #[serde(default = "default_mode")]
    pub mode: GoalMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl GoalPauseRequest {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() || self.id.len() > MAX_TEXT_BYTES {
            return Err(Error::new("invalid", "goal pause id must be nonempty"));
        }
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalClearRequest {
    pub id: String,
    pub agent: String,
    #[serde(default = "default_mode")]
    pub mode: GoalMode,
}

impl GoalClearRequest {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() || self.id.len() > MAX_TEXT_BYTES {
            return Err(Error::new("invalid", "goal clear id must be nonempty"));
        }
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalResumeRequest {
    pub id: String,
    pub agent: String,
    #[serde(default = "default_mode")]
    pub mode: GoalMode,
}

impl GoalResumeRequest {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() || self.id.len() > MAX_TEXT_BYTES {
            return Err(Error::new("invalid", "goal resume id must be nonempty"));
        }
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
            || response.id != self.id
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
                if response
                    .command_id
                    .as_deref()
                    .is_none_or(|id| id.trim().is_empty() || id == self.id)
                {
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
                if Some(continuation.command_id.as_str()) != response.command_id.as_deref()
                    || continuation.agent.as_deref() != Some(self.agent.as_str())
                    || continuation.generation != Some(response.view.generation)
                    || continuation.linked_command.is_some()
                {
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

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalRoundOutcome {
    Completed,
    Blocked,
    Continued,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalRoundResponse {
    /// The original round mutation id, separate from its optional SEND id.
    pub id: String,
    pub outcome: GoalRoundOutcome,
    /// Blocked only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Continued only; the distinct SEND id frozen when the round was claimed.
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
