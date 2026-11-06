//! Universal request/answer syntax shared by CLI, API, rules, Store and plugins.
//!
//! This module owns JSON syntax only: field names, types and unknown-field
//! rejection. Answer values, allowed options and cardinality stay plugin-owned:
//! each plugin validates a reply against the exact pending native offer before
//! any native write. Optional native constraints that are absent stay absent;
//! nothing here invents minimums, maximums, required flags or empty bans.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Answer shape of one universal question.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuestionKind {
    Text,
    Single,
    Multiple,
    Number,
    Boolean,
}

/// One offered universal option. Answers name `id`; where the native offer
/// carries labels only, the plugin maps the native label to `id`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct QuestionOption {
    pub id: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// One universal question. Absent native constraints stay absent: `options`
/// is empty for free-shape questions, and `min/max_selections` are present
/// only when the native offer states them. Bounds are unsigned, so negative
/// values reject at syntax.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Question {
    pub id: String,
    pub kind: QuestionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    #[serde(default)]
    pub options: Vec<QuestionOption>,
    #[serde(default)]
    pub free_entry: bool,
    #[serde(default)]
    pub is_secret: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_selections: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_selections: Option<u64>,
}

/// Universal public details of a question request. `is_secret` keeps the
/// pre-persistence Store secret guard effective.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct QuestionRequest {
    pub questions: Vec<Question>,
}

/// Universal answer to one question: the canonical string list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct Answer {
    pub answers: Vec<String>,
}

/// Bare answers map keyed by question id (`{question: {answers: [string]}}`).
pub type Answers = BTreeMap<String, Answer>;

/// Owning syntax parse for the bare answers map.
pub fn parse_answers_map(value: &Value) -> Result<Answers> {
    serde_json::from_value(value.clone())
        .map_err(|_| Error::new("invalid", "answers must be an object keyed by question id"))
}

/// Universal `answer` command args (`{"answers": {question: {answers: [string]}}}`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct AnswerArgs {
    pub answers: Answers,
}

impl AnswerArgs {
    /// Owning syntax parse for `answer` command args. Foreign answer shapes
    /// reject here, before persistence.
    pub fn parse(args: &Value) -> Result<Self> {
        serde_json::from_value(args.clone()).map_err(|_| {
            Error::new(
                "invalid",
                "answer args must be {\"answers\": {question: {answers: [string]}}}",
            )
        })
    }
}

/// Which reply verb decides one universal approval choice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    Approve,
    Deny,
}

/// One offered universal approval choice.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct ApprovalChoice {
    pub id: String,
    pub label: String,
    pub decision: ApprovalDecision,
}

/// Universal public details of an approval request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub struct ApprovalRequest {
    pub summary: String,
    pub choices: Vec<ApprovalChoice>,
    #[serde(default)]
    pub feedback_allowed: bool,
}
