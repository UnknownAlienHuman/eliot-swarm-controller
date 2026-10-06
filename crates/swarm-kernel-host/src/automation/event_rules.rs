//! Typed selectors over durable system observations.
//!
//! ScriptRun selectors intentionally name only the durable observation source
//! and kind plus an optional closed status filter. They never name methods,
//! arbitrary JSON paths, payload fields, or executable text. Unknown but
//! syntactically valid source/kind pairs remain saved so a manager can prepare
//! a route before its producer exists; they match only after a Store event
//! adapter can produce a safe projection for an exact observation.

use super::{
    actions::{AutomationCause, AutomationStep},
    intake::{EventReceipt, LocalProducer},
};
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

pub(crate) const MAX_EVENT_RULES: usize = 16;
const MAX_SELECTOR_NAME_BYTES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventRuleSource {
    #[serde(rename = "task.submission")]
    TaskSubmission,
}

impl EventRuleSource {
    fn identity(self) -> (&'static str, &'static str) {
        match self {
            Self::TaskSubmission => ("controller", "task.submission"),
        }
    }
}

/// Legacy predicate retained for the pre-generic TaskSubmission rule format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventRulePredicate {
    Applied,
}

/// Only metadata explicitly normalized by a Store event adapter can satisfy
/// this filter. Payload text is never searched or forwarded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventStatus {
    Applied,
    Completed,
    Failed,
    Incomplete,
    Cancelled,
    Rejected,
    Sent,
    Answered,
    Invalidated,
    Unknown,
}

impl EventStatus {
    pub(crate) fn parse_optional(value: Option<&str>) -> Result<Option<Self>> {
        value
            .map(|value| match value {
                "applied" => Ok(Self::Applied),
                "completed" => Ok(Self::Completed),
                "failed" => Ok(Self::Failed),
                "incomplete" => Ok(Self::Incomplete),
                "cancelled" => Ok(Self::Cancelled),
                "rejected" => Ok(Self::Rejected),
                "sent" => Ok(Self::Sent),
                "answered" => Ok(Self::Answered),
                "invalidated" => Ok(Self::Invalidated),
                "unknown" => Ok(Self::Unknown),
                _ => Err(Error::invalid(
                    "event status is not a supported normalized value",
                )),
            })
            .transpose()
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Incomplete => "incomplete",
            Self::Cancelled => "cancelled",
            Self::Rejected => "rejected",
            Self::Sent => "sent",
            Self::Answered => "answered",
            Self::Invalidated => "invalidated",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventRuleAction {
    ReviewDispatch,
    ScriptRun,
}

impl EventRuleAction {
    pub(crate) const fn step(self) -> AutomationStep {
        match self {
            Self::ReviewDispatch => AutomationStep::ReviewDispatch,
            Self::ScriptRun => AutomationStep::ScriptRun,
        }
    }
}

/// A manager-selected event route. The old `source`/`predicate` form remains
/// readable for stored TaskSubmission ReviewDispatch and ScriptRun rules.
/// New ScriptRun rules use `source_id` and `event_kind` directly.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EventRule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) source: Option<EventRuleSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) predicate: Option<EventRulePredicate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) source_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) event_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) status: Option<EventStatus>,
    pub(crate) action: EventRuleAction,
}

impl EventRule {
    pub(crate) fn selected_source_kind(&self) -> Option<(&str, &str)> {
        match (&self.source, &self.source_id, &self.event_kind) {
            (Some(source), None, None) => {
                let (source_id, event_kind) = source.identity();
                Some((source_id, event_kind))
            }
            (None, Some(source_id), Some(event_kind)) => Some((source_id, event_kind)),
            _ => None,
        }
    }

    pub(crate) fn is_task_submission_applied(&self) -> bool {
        self.selected_source_kind() == Some(("controller", "task.submission"))
            && (self.predicate == Some(EventRulePredicate::Applied)
                || self.status == Some(EventStatus::Applied))
    }

    pub(crate) fn matches_receipt(&self, receipt: &EventReceipt, cause: &AutomationCause) -> bool {
        let Some((source_id, event_kind)) = self.selected_source_kind() else {
            return false;
        };
        let receipt_source_matches = source_id == LocalProducer::TaskSubmission.stream_id()
            && event_kind == LocalProducer::TaskSubmission.event_kind()
            && receipt.source_id == LocalProducer::TaskSubmission.source_id();
        if self.action != EventRuleAction::ReviewDispatch
            && self.action != EventRuleAction::ScriptRun
        {
            return false;
        }
        let AutomationCause::AppliedSubmission {
            observation_id,
            operation_id,
            submission_ref,
        } = cause
        else {
            return false;
        };
        let legacy_applied = self.predicate == Some(EventRulePredicate::Applied);
        (legacy_applied || self.status == Some(EventStatus::Applied))
            && receipt_source_matches
            && event_kind == receipt.event_kind
            && receipt.observation_id == *observation_id
            && receipt.operation_id.as_deref() == Some(operation_id.as_str())
            && receipt.payload["outcome"] == "applied"
            && receipt.payload["operation_id"].as_str() == Some(operation_id.as_str())
            && receipt.payload["submission_ref"].as_str() == Some(submission_ref.as_str())
    }

    pub(crate) fn matches_safe_event(
        &self,
        source_id: &str,
        event_kind: &str,
        status: Option<EventStatus>,
    ) -> bool {
        self.action == EventRuleAction::ScriptRun
            && self.selected_source_kind() == Some((source_id, event_kind))
            && self.status.is_none_or(|expected| status == Some(expected))
            && self.predicate.is_none()
    }

    pub(crate) fn selects_script_run_source_kind(&self, source_id: &str, event_kind: &str) -> bool {
        self.action == EventRuleAction::ScriptRun
            && self.selected_source_kind() == Some((source_id, event_kind))
    }

    pub(crate) fn validate_shape(&self) -> Result<()> {
        let legacy = self.source.is_some() || self.predicate.is_some();
        let generic = self.source_id.is_some() || self.event_kind.is_some();
        if legacy == generic {
            return Err(Error::invalid(
                "event rule must use exactly one of the legacy source/predicate pair or source_id/event_kind selector",
            ));
        }
        if legacy {
            if self.source.is_none()
                || self.predicate.is_none()
                || self.status.is_some()
                || self.action == EventRuleAction::ReviewDispatch
                    && (self.source != Some(EventRuleSource::TaskSubmission)
                        || self.predicate != Some(EventRulePredicate::Applied))
            {
                return Err(Error::invalid(
                    "legacy event rules require a registered source and predicate",
                ));
            }
            return Ok(());
        }
        let source_id = self.source_id.as_deref().unwrap_or_default();
        let event_kind = self.event_kind.as_deref().unwrap_or_default();
        if !valid_selector_name(source_id) || !valid_selector_name(event_kind) {
            return Err(Error::invalid(
                "event selectors require bounded nonempty source_id and event_kind values",
            ));
        }
        if self.action == EventRuleAction::ReviewDispatch
            && (source_id != "controller" || event_kind != "task.submission")
        {
            return Err(Error::invalid(
                "ReviewDispatch supports only the typed TaskSubmission source",
            ));
        }
        if self.action == EventRuleAction::ReviewDispatch
            && self.status != Some(EventStatus::Applied)
        {
            return Err(Error::invalid(
                "ReviewDispatch requires the applied TaskSubmission status",
            ));
        }
        Ok(())
    }
}

fn valid_selector_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SELECTOR_NAME_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-/@".contains(&byte))
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
    for rule in rules {
        rule.validate_shape()?;
    }
    let unique = rules.iter().cloned().collect::<BTreeSet<_>>();
    if unique.len() != rules.len() {
        return Err(Error::invalid("event_rules must not contain duplicates"));
    }
    Ok(())
}

pub(crate) fn parse_settings(value: &Value) -> Result<Option<Vec<EventRule>>> {
    if value.is_null() {
        return Ok(None);
    }
    let rules: Vec<EventRule> = serde_json::from_value(value.clone()).map_err(|_| {
        Error::invalid("event_rules must contain registered event selectors and actions")
    })?;
    validate_structure(&rules)?;
    Ok(Some(rules))
}
