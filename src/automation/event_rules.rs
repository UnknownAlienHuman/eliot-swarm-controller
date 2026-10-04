//! Closed event-to-action rules for the first Store-backed typed source.
//!
//! A rule is a typed relationship between one registered local event and an
//! already selected automation action. It cannot name methods, executable
//! text, or arbitrary event fields.

use super::{
    actions::{AutomationCause, AutomationStep},
    intake::{EventReceipt, LocalProducer},
};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub(crate) const MAX_EVENT_RULES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventRuleSource {
    #[serde(rename = "task.submission")]
    TaskSubmission,
}

impl EventRuleSource {
    fn producer(self) -> LocalProducer {
        match self {
            Self::TaskSubmission => LocalProducer::TaskSubmission,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventRulePredicate {
    Applied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventRuleAction {
    ReviewDispatch,
}

impl EventRuleAction {
    pub(crate) const fn step(self) -> AutomationStep {
        match self {
            Self::ReviewDispatch => AutomationStep::ReviewDispatch,
        }
    }
}

/// One closed source/predicate/action combination. `event_rules: null` keeps
/// the legacy TaskSubmission-to-ReviewDispatch route for saved entries that
/// predate typed event settings; an empty array explicitly selects no event
/// route while leaving the ordinary action available for direct Manager use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EventRule {
    pub(crate) source: EventRuleSource,
    pub(crate) predicate: EventRulePredicate,
    pub(crate) action: EventRuleAction,
}

impl EventRule {
    pub(crate) fn matches(&self, receipt: &EventReceipt, cause: &AutomationCause) -> bool {
        let producer = self.source.producer();
        let AutomationCause::AppliedSubmission {
            observation_id,
            operation_id,
            submission_ref,
        } = cause
        else {
            return false;
        };

        self.predicate == EventRulePredicate::Applied
            && self.action == EventRuleAction::ReviewDispatch
            && receipt.source_id == producer.source_id()
            && receipt.event_kind == producer.event_kind()
            && receipt.observation_id == *observation_id
            && receipt.operation_id.as_deref() == Some(operation_id.as_str())
            && receipt.payload["outcome"] == "applied"
            && receipt.payload["operation_id"].as_str() == Some(operation_id.as_str())
            && receipt.payload["submission_ref"].as_str() == Some(submission_ref.as_str())
    }
}

pub(crate) fn validate(rules: &[EventRule], selected_steps: &[AutomationStep]) -> Result<()> {
    validate_structure(rules)?;
    if rules
        .iter()
        .any(|rule| !selected_steps.contains(&rule.action.step()))
    {
        return Err(Error::invalid(
            "each event_rules action must also be present in the selected steps",
        ));
    }
    Ok(())
}

fn validate_structure(rules: &[EventRule]) -> Result<()> {
    if rules.len() > MAX_EVENT_RULES {
        return Err(Error::invalid(format!(
            "event_rules exceeds the supported bound of {MAX_EVENT_RULES}"
        )));
    }
    let unique = rules.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != rules.len() {
        return Err(Error::invalid("event_rules must not contain duplicates"));
    }
    Ok(())
}

pub(crate) fn parse_settings(value: &serde_json::Value) -> Result<Option<Vec<EventRule>>> {
    if value.is_null() {
        return Ok(None);
    }
    let rules: Vec<EventRule> = serde_json::from_value(value.clone())
        .map_err(|_| Error::invalid("event_rules must contain only registered typed rules"))?;
    validate_structure(&rules)?;
    Ok(Some(rules))
}
