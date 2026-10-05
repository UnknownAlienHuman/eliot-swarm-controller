//! Store-independent event rule validation and ScriptRun selector matching.
//!
//! The Store must authorize and normalize an observation before constructing
//! `EventMetadata`; this module only matches that safe header to a saved rule.

use crate::{
    EventMetadata, EventSelector, EventStatus, Result, ScriptError, canonical_json, sha256_hex,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_EVENT_RULES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventAction {
    ReviewDispatch,
    ScriptRun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyEventSource {
    #[serde(rename = "task.submission")]
    TaskSubmission,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyPredicate {
    Applied,
}

/// Serialized compatibly with Store `event_rules` records. Unknown bounded
/// source/kind pairs are retained so future Store adapters can support them.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRule {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<LegacyEventSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<LegacyPredicate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    pub action: EventAction,
}

impl EventRule {
    pub fn selector(&self) -> Result<EventSelector> {
        let (source_id, event_kind, status) = match (
            &self.source,
            &self.predicate,
            &self.source_id,
            &self.event_kind,
        ) {
            (
                Some(LegacyEventSource::TaskSubmission),
                Some(LegacyPredicate::Applied),
                None,
                None,
            ) if self.status.is_none() => {
                ("controller", "task.submission", Some(EventStatus::Applied))
            }
            (None, None, Some(source_id), Some(event_kind)) => {
                (source_id, event_kind, parse_status(self.status.as_deref())?)
            }
            _ => return Err(ScriptError::new("INVALID_PARAMS")),
        };
        EventSelector::new(source_id, event_kind, status).map_err(Into::into)
    }

    pub fn matches(&self, event: &EventMetadata) -> Result<bool> {
        if self.action != EventAction::ScriptRun {
            return Ok(false);
        }
        let legacy_applied = self.source == Some(LegacyEventSource::TaskSubmission)
            && self.predicate == Some(LegacyPredicate::Applied);
        if self.predicate.is_some() && !legacy_applied {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
        if self.source.is_some() != self.predicate.is_some() {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
        if self.source.is_some() && (self.source_id.is_some() || self.event_kind.is_some()) {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
        let selector = self.selector()?;
        if legacy_applied {
            return Ok(event.source_id() == "controller"
                && event.event_kind() == "task.submission"
                && event.status() == Some(EventStatus::Applied)
                && selector.matches(event));
        }
        Ok(selector.matches(event))
    }

    fn validate_shape(&self) -> Result<()> {
        let legacy = self.source.is_some() || self.predicate.is_some();
        let generic = self.source_id.is_some() || self.event_kind.is_some();
        if legacy == generic {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
        if legacy {
            if self.source != Some(LegacyEventSource::TaskSubmission)
                || self.predicate != Some(LegacyPredicate::Applied)
                || self.status.is_some()
            {
                return Err(ScriptError::new("INVALID_PARAMS"));
            }
            if self.action == EventAction::ReviewDispatch {
                return Ok(());
            }
        }
        if self.action == EventAction::ReviewDispatch {
            let selector = self.selector()?;
            if selector.source_id() != "controller"
                || selector.event_kind() != "task.submission"
                || selector.status() != Some(EventStatus::Applied)
            {
                return Err(ScriptError::new("INVALID_PARAMS"));
            }
        }
        if self.predicate.is_some() && self.action != EventAction::ScriptRun {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
        let _ = self.selector()?;
        Ok(())
    }
}

pub fn validate_event_rules(rules: &[EventRule], selected_actions: &[EventAction]) -> Result<()> {
    if rules.len() > MAX_EVENT_RULES {
        return Err(ScriptError::new("INVALID_PARAMS"));
    }
    let mut unique = BTreeSet::new();
    for rule in rules {
        rule.validate_shape()?;
        if !selected_actions.contains(&rule.action) || !unique.insert(rule) {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
    }
    Ok(())
}

/// Stable occurrence identity used by Store cursor/journal deduplication.
/// Typed occurrence identity wins over observation identity when present.
pub fn semantic_event_id(
    event: &EventMetadata,
    occurrence_phase: Option<&str>,
    occurrence_id: Option<&str>,
) -> Result<String> {
    match (occurrence_phase, occurrence_id) {
        (Some(phase), Some(occurrence_id))
            if valid_occurrence_identity(phase) && valid_occurrence_identity(occurrence_id) =>
        {
            let identity = serde_json::json!({"phase":phase,"occurrence_id":occurrence_id});
            Ok(sha256_hex(canonical_json(&identity)?.as_bytes()))
        }
        (None, None) => Ok(format!("observation:{}", event.observation_id())),
        _ => Err(ScriptError::new("SCRIPT_EVENT_PROJECTION_INVALID")),
    }
}

fn valid_occurrence_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-/@".contains(&byte))
}

fn parse_status(value: Option<&str>) -> Result<Option<EventStatus>> {
    value
        .map(|value| EventStatus::parse(value).map_err(Into::into))
        .transpose()
}
