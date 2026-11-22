//! Bounded completion wait over a frozen set of child command/turn ids.
//!
//! The captured set is frozen at request time: later turns or new children
//! never join it. A child being idle is not success; each entry reports the
//! terminal outcome of its exact captured command or turn.

use super::ChildRunFacts;
use crate::{Error, Result, TurnResolutionSource};
use serde::{Deserialize, Serialize};

/// Maximum number of entries in one wait (commands plus turns).
pub const MAX_SWARM_WAIT_TARGETS: usize = 256;
/// Maximum server wait, matching `events.subscribe` (`LIVE_WAIT_MAX_MS`).
pub const MAX_SWARM_WAIT_MS: u64 = 30_000;
/// Per-entry bound for retained `final_text` returned by `swarm.wait`.
pub const MAX_WAIT_FINAL_TEXT_BYTES: usize = 16 * 1024;
/// Total bound for retained `final_text` across one `swarm.wait` response,
/// the same as one retained turn text (`MAX_EVENT_BYTES`).
pub const MAX_WAIT_FRAME_BYTES: usize = 64 * 1024;
/// Total serialized child facts allowed across one wait response.
pub const MAX_WAIT_FACTS_FRAME_BYTES: usize = 3 * 1024 * 1024;

/// Exact turn binding for one wait entry.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitTurnRef {
    #[serde(deserialize_with = "required_id")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub agent: String,
    #[serde(deserialize_with = "generation")]
    #[schemars(range(min = 0))]
    pub generation: i64,
    #[serde(deserialize_with = "required_id")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub turn_id: String,
}

impl WaitTurnRef {
    pub fn new(agent: impl Into<String>, generation: i64, turn_id: impl Into<String>) -> Self {
        Self {
            agent: agent.into(),
            generation,
            turn_id: turn_id.into(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_id(&self.agent)?;
        validate_generation(self.generation)?;
        validate_id(&self.turn_id)
    }

    /// Canonical echo used in responses: `agent/generation/turn_id`.
    pub fn canonical_id(&self) -> String {
        format!("{}/{}/{}", self.agent, self.generation, self.turn_id)
    }
}

/// Bounded wait over an explicitly captured set of child command ids and/or
/// exact `(agent, generation, turn)` bindings.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmWaitRequest {
    #[serde(default, deserialize_with = "super::deserialize_string_list")]
    #[schemars(
        with = "super::string_list::StringListInput",
        description = "One command id or an array of command ids."
    )]
    pub commands: Vec<String>,
    #[serde(default)]
    pub turns: Vec<WaitTurnRef>,
    /// `all` completes when every entry is terminal; `any` completes when any
    /// entry is terminal.
    #[serde(default = "all_wait_mode")]
    #[schemars(description = "Wait mode: all (default) or any.")]
    pub mode: String,
    #[serde(default, deserialize_with = "wait_ms")]
    #[schemars(
        range(min = 0),
        description = "Unsigned milliseconds; values above 30000 are accepted and clamped to 30000."
    )]
    pub wait_ms: u64,
    /// End the wait when the calling Agent receives new mail. Agent callers
    /// only; the owner has no mailbox.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    #[schemars(
        description = "Agent callers only: also end the wait when mail arrives in the caller's own mailbox after the wait starts."
    )]
    pub mailbox: bool,
}

pub(super) fn all_wait_mode() -> String {
    "all".to_owned()
}

impl SwarmWaitRequest {
    pub fn new(
        commands: Vec<String>,
        turns: Vec<WaitTurnRef>,
        mode: impl Into<String>,
        wait_ms: u64,
    ) -> Self {
        Self {
            commands,
            turns,
            mode: mode.into(),
            wait_ms,
            mailbox: false,
        }
    }

    pub fn with_mailbox(mut self, mailbox: bool) -> Self {
        self.mailbox = mailbox;
        self
    }

    pub fn total(&self) -> usize {
        self.commands.len().saturating_add(self.turns.len())
    }

    pub fn validate(&self) -> Result<()> {
        if self.mode != "all" && self.mode != "any" {
            return Err(Error::new("invalid", "mode must be all or any"));
        }
        let total = self.total();
        if total == 0 || total > MAX_SWARM_WAIT_TARGETS {
            return Err(Error::new(
                "invalid",
                "wait requires 1..=256 captured entries",
            ));
        }
        for id in &self.commands {
            validate_id(id)?;
        }
        for turn in &self.turns {
            turn.validate()?;
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &SwarmWaitResponse) -> Result<()> {
        if self.validate().is_err()
            || response.mode != self.mode
            || response.results.len() != self.total()
        {
            return Err(binding_mismatch());
        }
        // Commands come first in request order, then turns in request order.
        let mut expected: Vec<(String, String)> = Vec::new();
        for id in &self.commands {
            expected.push(("command".to_owned(), id.clone()));
        }
        for turn in &self.turns {
            expected.push(("turn".to_owned(), turn.canonical_id()));
        }
        for (result, (kind, id)) in response.results.iter().zip(expected.iter()) {
            if &result.kind != kind || &result.id != id || result.validate().is_err() {
                return Err(binding_mismatch());
            }
        }
        if response.completed && response.timed_out {
            return Err(binding_mismatch());
        }
        // Completion means the mode condition holds over terminal entries.
        let terminal = response
            .results
            .iter()
            .filter(|entry| entry.status != "still_running")
            .count();
        let succeeded = response
            .results
            .iter()
            .filter(|entry| entry.status == "succeeded")
            .count();
        let should_complete = match self.mode.as_str() {
            "all" => terminal == response.results.len(),
            _ => terminal > 0,
        };
        if response.completed != should_complete {
            return Err(binding_mismatch());
        }
        // A completed `all` wait has no still_running entries; a completed
        // `any` wait has at least one terminal entry.
        if response.completed && self.mode == "all" && terminal != response.results.len() {
            return Err(binding_mismatch());
        }
        if response.completed && self.mode == "any" && terminal == 0 {
            return Err(binding_mismatch());
        }
        let _ = succeeded;
        Ok(())
    }
}

/// One frozen entry result. Completed entries with a bound turn also carry
/// that turn's identity and its bounded retained `final_text`; `truncated`
/// marks a per-entry cut or a withheld tail once the response frame budget
/// is spent.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitEntryResult {
    /// `command` or `turn`.
    #[schemars(description = "Entry kind: command or turn.")]
    pub kind: String,
    /// Command id, or `agent/generation/turn_id` for turn entries.
    #[schemars(description = "Command id, or agent/generation/turn_id for turn entries.")]
    pub id: String,
    /// `succeeded`, `failed`, `interrupted`, `unknown`, `still_running`,
    /// `not_found` or `not_visible`.
    #[schemars(description = "Terminal outcome or wait state for this entry.")]
    pub status: String,
    /// Provenance of a direct turn or a command's bound turn, when qualified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution_source: Option<TurnResolutionSource>,
    /// Bound turn agent, when the entry resolved to a turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    /// Bound turn generation, when the entry resolved to a turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub generation: Option<i64>,
    /// Retained managed worktree branch for the visible child command or turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Error code if this entry's retained branch could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_error: Option<String>,
    /// Bound turn id, when the entry resolved to a turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    /// Bounded retained `final_text` of a completed bound turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-maxBytes" = MAX_WAIT_FINAL_TEXT_BYTES))]
    pub final_text: Option<String>,
    /// True when `final_text` was cut to its bound or withheld by the
    /// response frame budget; `agent.result` reads the whole text.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
    /// Bounded current-generation work facts for visible settled Agent results.
    #[serde(default)]
    #[schemars(extend("x-maxBytes" = super::MAX_CHILD_RUN_FACTS_BYTES))]
    pub facts: ChildRunFacts,
}

impl WaitEntryResult {
    pub fn new(kind: impl Into<String>, id: impl Into<String>, status: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            id: id.into(),
            status: status.into(),
            resolution_source: None,
            agent: None,
            generation: None,
            branch: None,
            branch_error: None,
            turn_id: None,
            final_text: None,
            truncated: false,
            facts: ChildRunFacts::unobserved(),
        }
    }

    /// Attach the bound turn identity to an entry result.
    pub fn with_turn(
        mut self,
        agent: impl Into<String>,
        generation: i64,
        turn_id: impl Into<String>,
    ) -> Self {
        self.agent = Some(agent.into());
        self.generation = Some(generation);
        self.turn_id = Some(turn_id.into());
        self
    }

    pub fn with_branch(mut self, branch: Result<Option<String>>) -> Self {
        match branch {
            Ok(branch) => self.branch = branch,
            Err(error) => self.branch_error = Some(error.code),
        }
        self
    }

    /// Attach retained `final_text` cut to the per-entry bound and to the
    /// `budget` bytes left in the response frame. A spent budget withholds
    /// the text and marks the entry `truncated`.
    pub fn with_final_text(mut self, text: &str, budget: usize) -> Self {
        let (kept, cut) = truncate_bytes(text, budget.min(MAX_WAIT_FINAL_TEXT_BYTES));
        self.truncated = cut;
        self.final_text = (!kept.is_empty() || !cut).then_some(kept);
        self
    }

    /// Attach work facts captured for this exact Agent generation.
    pub fn with_facts(mut self, facts: ChildRunFacts) -> Self {
        self.facts = facts;
        self
    }

    pub fn validate(&self) -> Result<()> {
        if self.kind != "command" && self.kind != "turn" {
            return Err(Error::new(
                "invalid",
                "wait entry kind must be command or turn",
            ));
        }
        if self.id.trim().is_empty() {
            return Err(Error::new("invalid", "wait entry id must be nonempty"));
        }
        if self.branch.is_some() && self.branch_error.is_some() {
            return Err(Error::new(
                "invalid",
                "wait branch conflicts with its read error",
            ));
        }
        match self.status.as_str() {
            "succeeded" | "failed" | "interrupted" | "unknown" | "still_running" | "not_found"
            | "not_visible" => Ok(()),
            _ => Err(Error::new("invalid", "unknown wait entry status")),
        }?;
        match (&self.agent, self.generation, &self.turn_id) {
            (None, None, None) => {}
            (Some(agent), Some(generation), Some(turn_id)) => {
                validate_id(agent)?;
                validate_generation(generation)?;
                validate_id(turn_id)?;
            }
            _ => {
                return Err(Error::new(
                    "invalid",
                    "wait entry turn binding needs agent, generation and turn_id together",
                ));
            }
        }
        if self
            .final_text
            .as_deref()
            .is_some_and(|text| text.len() > MAX_WAIT_FINAL_TEXT_BYTES)
        {
            return Err(Error::new(
                "invalid",
                "wait entry final_text exceeds its bound",
            ));
        }
        if (self.final_text.is_some() || self.truncated) && self.agent.is_none() {
            return Err(Error::new(
                "invalid",
                "wait entry final_text needs its turn binding",
            ));
        }
        self.facts.validate()?;
        if (self.agent.is_none()
            || !matches!(self.status.as_str(), "succeeded" | "failed" | "interrupted"))
            && !self.facts.is_unobserved()
        {
            return Err(Error::new(
                "invalid",
                "wait facts require a visible settled Agent result",
            ));
        }
        Ok(())
    }

    pub fn is_terminal(&self) -> bool {
        self.status != "still_running"
    }

    pub fn succeeded(&self) -> bool {
        self.status == "succeeded"
    }
}

/// Bounded wait response. `completed` mirrors the requested mode condition;
/// `timed_out` is true only when the deadline expired first. `mail_wake` is
/// true when a `mailbox` wait returned early on new mail for the caller.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmWaitResponse {
    pub mode: String,
    pub completed: bool,
    pub timed_out: bool,
    #[serde(default)]
    pub mail_wake: bool,
    pub results: Vec<WaitEntryResult>,
}

impl SwarmWaitResponse {
    pub fn validate(&self) -> Result<()> {
        if self.mode != "all" && self.mode != "any" {
            return Err(Error::new("invalid", "mode must be all or any"));
        }
        if self.results.len() > MAX_SWARM_WAIT_TARGETS {
            return Err(Error::new(
                "invalid",
                "wait response exceeds the entry bound",
            ));
        }
        for entry in &self.results {
            entry.validate()?;
        }
        if self.completed && self.timed_out {
            return Err(Error::new("invalid", "wait cannot complete and time out"));
        }
        if self.mail_wake && self.timed_out {
            return Err(Error::new("invalid", "mail wake is not a timeout"));
        }
        let frame: usize = self
            .results
            .iter()
            .filter_map(|entry| entry.final_text.as_deref())
            .map(|text| text.len())
            .sum();
        if frame > MAX_WAIT_FRAME_BYTES {
            return Err(Error::new(
                "invalid",
                "wait response exceeds its final_text frame budget",
            ));
        }
        let facts_frame = serde_json::to_vec(
            &self
                .results
                .iter()
                .map(|entry| &entry.facts)
                .collect::<Vec<_>>(),
        )
        .map_err(|_| Error::new("invalid", "wait facts could not be encoded"))?
        .len();
        if facts_frame > MAX_WAIT_FACTS_FRAME_BYTES {
            return Err(Error::new(
                "invalid",
                "wait response exceeds its child facts frame budget",
            ));
        }
        Ok(())
    }
}

/// Cut `text` to at most `bound` bytes on a UTF-8 boundary. Returns the kept
/// prefix and whether anything was cut.
pub fn truncate_bytes(text: &str, bound: usize) -> (String, bool) {
    if text.len() <= bound {
        return (text.to_owned(), false);
    }
    let mut end = bound;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    (text[..end].to_owned(), true)
}

fn validate_id(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new(
            "invalid",
            "wait identifiers must be non-empty strings",
        ));
    }
    Ok(())
}

fn validate_generation(value: i64) -> Result<()> {
    if value < 0 {
        return Err(Error::new(
            "invalid",
            "generation must be a nonnegative integer",
        ));
    }
    Ok(())
}

fn required_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let id = String::deserialize(deserializer)?;
    validate_id(&id).map_err(serde::de::Error::custom)?;
    Ok(id)
}

fn generation<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<i64, D::Error> {
    let value = i64::deserialize(deserializer)?;
    validate_generation(value).map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn wait_ms<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<u64, D::Error> {
    u64::deserialize(deserializer)
}

fn binding_mismatch() -> Error {
    Error::new(
        "unknown",
        "read response does not match the requested binding",
    )
}
